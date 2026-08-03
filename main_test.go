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