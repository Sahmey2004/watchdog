package main

import (
	"testing"
	"time"
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

func TestSuperviseSetsStatusTransitions(t *testing.T) {
	sup := NewSupervisor()

	go Supervise(sup, "test-exit", false, "sh", "-c", "exit 0")

	deadline := time.Now().Add(2 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		last = sup.Statuses()["test-exit"]
		if last == "exited" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected status %q for test-exit within 2s, last seen %q", "exited", last)
}