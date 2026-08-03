package main

import (
	"testing"
	"time"
)

func TestRecordRestartAndMetrics(t *testing.T) {
	sup := NewSupervisor()

	sup.RecordRestart("hanging-worker", 2*time.Second)
	sup.RecordRestart("hanging-worker", 1*time.Second)

	got := sup.Metrics()
	m, ok := got["hanging-worker"]
	if !ok {
		t.Fatalf("expected hanging-worker to have metrics, got %v", got)
	}
	if m.RestartCount != 2 {
		t.Errorf("expected RestartCount 2, got %d", m.RestartCount)
	}
	if m.TotalDowntime != 3*time.Second {
		t.Errorf("expected TotalDowntime 3s, got %v", m.TotalDowntime)
	}
	if m.LastDowntime != 1*time.Second {
		t.Errorf("expected LastDowntime 1s (the most recent incident), got %v", m.LastDowntime)
	}
}

func TestMetricsEmptyForUnknownService(t *testing.T) {
	sup := NewSupervisor()
	got := sup.Metrics()
	if len(got) != 0 {
		t.Errorf("expected no metrics before any RecordRestart call, got %v", got)
	}
}

// TestSuperviseRecordsRestartMetrics verifies Supervise() itself — not
// just RecordRestart in isolation — populates metrics after a real
// restart: a service crashes, and the next successful start should record
// one incident with a nonzero downtime.
func TestSuperviseRecordsRestartMetrics(t *testing.T) {
	sup := NewSupervisor()

	go Supervise(sup, "test-crash", false, "sh", "-c", "exit 1")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m := sup.Metrics()["test-crash"]
		if m.RestartCount >= 1 {
			if m.LastDowntime <= 0 {
				t.Fatalf("expected a nonzero downtime for the recorded restart, got %v", m.LastDowntime)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected at least one restart to be recorded for test-crash within 3s, got %v", sup.Metrics()["test-crash"])
}
