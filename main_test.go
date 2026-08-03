package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHeartbeatAndCheckStale(t *testing.T) {
	sup := NewSupervisor()

	// A service that just checked in should NOT be considered stale.
	sup.Heartbeat("echo-bot")
	stale := sup.CheckStale(5 * time.Second)
	if len(stale) != 0 {
		t.Errorf("expected no stale services right after a heartbeat, got %v", stale)
	}

	// Simulate a service that went quiet 10 seconds ago, without
	// actually waiting 10 real seconds — we just write an old
	// timestamp directly into the map ourselves.
	sup.mu.Lock()
	sup.services["ghost-service"] = time.Now().Add(-10 * time.Second)
	sup.mu.Unlock()

	stale = sup.CheckStale(5 * time.Second)
	if len(stale) != 1 || stale[0] != "ghost-service" {
		t.Errorf("expected [ghost-service] to be stale, got %v", stale)
	}
}

func TestSetStatusAndStatuses(t *testing.T) {
	sup := NewSupervisor()

	sup.SetStatus("hanging-worker", "running")
	sup.SetStatus("flaky-crasher", "crashed")

	got := sup.Statuses()
	want := map[string]string{
		"hanging-worker": "running",
		"flaky-crasher":  "crashed",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d statuses, got %d: %v", len(want), len(got), got)
	}
	for name, status := range want {
		if got[name] != status {
			t.Errorf("expected %s to have status %q, got %q", name, status, got[name])
		}
	}

	// Statuses() must return a copy — mutating it must not affect the
	// Supervisor's internal state.
	got["hanging-worker"] = "tampered"
	sup.mu.Lock()
	internal := sup.status["hanging-worker"]
	sup.mu.Unlock()
	if internal != "running" {
		t.Errorf("expected internal status to remain %q, got %q — Statuses() leaked the live map", "running", internal)
	}
}

func TestSetStatusBroadcastsToHub(t *testing.T) {
	sup := NewSupervisor()
	hub := NewHub()
	sup.SetHub(hub)
	ch := hub.Register()

	sup.SetStatus("hanging-worker", "running")

	select {
	case payload := <-ch:
		var got map[string]string
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("failed to unmarshal broadcast payload: %v", err)
		}
		if got["hanging-worker"] != "running" {
			t.Errorf("expected broadcast to contain hanging-worker=running, got %v", got)
		}
	default:
		t.Error("expected SetStatus to broadcast to the registered client, got no message")
	}
}

func TestSetStatusWithNoHubDoesNotPanic(t *testing.T) {
	sup := NewSupervisor()
	sup.SetStatus("hanging-worker", "running")
	if got := sup.Statuses()["hanging-worker"]; got != "running" {
		t.Errorf("expected status to be recorded even with no hub attached, got %q", got)
	}
}

func TestSuperviseSetsStatusTransitions(t *testing.T) {
	sup := NewSupervisor()

	go Supervise(sup, "test-exit", false, "sh", "-c", "sleep 0.1 && exit 0")

	deadline := time.Now().Add(2 * time.Second)
	var last string
	sawRunning := false
	for time.Now().Before(deadline) {
		last = sup.Statuses()["test-exit"]
		if last == "running" {
			sawRunning = true
		}
		if last == "exited" {
			if !sawRunning {
				t.Fatalf("expected status %q to be observed before %q, but it never was", "running", "exited")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected status %q for test-exit within 2s, last seen %q", "exited", last)
}

// TestRoutesWiring verifies that routes(sup) actually wires up /status and
// /dashboard at the exact path strings a real HTTP client (or the
// dashboard's own embedded JS, which calls fetch('/status')) would use.
// Calling the handler functions directly, as other tests do, bypasses the
// mux and would miss a path-string mismatch between Go and the embedded
// JS — this test exercises real routing end-to-end via httptest.NewServer.
func TestRoutesWiring(t *testing.T) {
	sup := NewSupervisor()
	sup.SetStatus("test-service", "running")
	server := httptest.NewServer(routes(sup, NewHub()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from /status, got %d", resp.StatusCode)
	}
	var statuses map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		t.Fatalf("failed to decode /status response: %v", err)
	}
	if statuses["test-service"] != "running" {
		t.Errorf("expected test-service to be running, got %v", statuses)
	}

	resp2, err := http.Get(server.URL + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from /dashboard, got %d", resp2.StatusCode)
	}

	resp3, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("expected 200 from /metrics, got %d", resp3.StatusCode)
	}
}

// TestWebSocketPushesStatusUpdates connects a real WebSocket client to a
// real server (via httptest.NewServer) and verifies two things end to
// end: a fresh connection immediately receives the current snapshot, and
// a later SetStatus call pushes an updated snapshot without the client
// polling for it — the actual behavior Stage 4b exists to add.
func TestWebSocketPushesStatusUpdates(t *testing.T) {
	sup := NewSupervisor()
	hub := NewHub()
	sup.SetHub(hub)
	sup.SetStatus("hanging-worker", "starting")

	server := httptest.NewServer(routes(sup, hub))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to dial %s: %v", wsURL, err)
	}
	defer conn.Close()

	_, initial, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read initial snapshot: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal(initial, &got); err != nil {
		t.Fatalf("failed to unmarshal initial snapshot: %v", err)
	}
	if got["hanging-worker"] != "starting" {
		t.Fatalf("expected initial snapshot to show hanging-worker=starting, got %v", got)
	}

	sup.SetStatus("hanging-worker", "running")

	_, update, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read pushed update: %v", err)
	}
	if err := json.Unmarshal(update, &got); err != nil {
		t.Fatalf("failed to unmarshal pushed update: %v", err)
	}
	if got["hanging-worker"] != "running" {
		t.Fatalf("expected pushed update to show hanging-worker=running, got %v", got)
	}
}

func TestStatusHandler(t *testing.T) {
	sup := NewSupervisor()
	sup.SetStatus("hanging-worker", "running")
	sup.SetStatus("flaky-crasher", "crashed")

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()

	statusHandler(sup)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var got map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	want := map[string]string{
		"hanging-worker": "running",
		"flaky-crasher":  "crashed",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d statuses, got %d: %v", len(want), len(got), got)
	}
	for name, status := range want {
		if got[name] != status {
			t.Errorf("expected %s to have status %q, got %q", name, status, got[name])
		}
	}
}

func TestMetricsHandler(t *testing.T) {
	sup := NewSupervisor()
	sup.RecordRestart("hanging-worker", 2*time.Second)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	metricsHandler(sup)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var got map[string]ServiceMetrics
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	m, ok := got["hanging-worker"]
	if !ok {
		t.Fatalf("expected hanging-worker in response, got %v", got)
	}
	if m.RestartCount != 1 {
		t.Errorf("expected RestartCount 1, got %d", m.RestartCount)
	}
}

func TestDashboardHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()

	dashboardHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html" {
		t.Errorf("expected Content-Type text/html, got %q", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{"/status", "setInterval", "background: #000", "color: #fff", "/ws", "WebSocket", "/metrics", "fetchMetrics"} {
		if !strings.Contains(body, want) {
			t.Errorf("expected dashboard HTML to contain %q", want)
		}
	}
}