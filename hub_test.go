package main

import (
	"testing"
	"time"
)

func TestHubBroadcastDeliversToAllClients(t *testing.T) {
	hub := NewHub()
	ch1 := hub.Register()
	ch2 := hub.Register()

	hub.Broadcast([]byte("hello"))

	select {
	case got := <-ch1:
		if string(got) != "hello" {
			t.Errorf("client 1: expected %q, got %q", "hello", got)
		}
	default:
		t.Error("client 1: expected a message, got none")
	}

	select {
	case got := <-ch2:
		if string(got) != "hello" {
			t.Errorf("client 2: expected %q, got %q", "hello", got)
		}
	default:
		t.Error("client 2: expected a message, got none")
	}
}

func TestHubBroadcastSkipsFullChannelWithoutBlocking(t *testing.T) {
	hub := NewHub()
	ch := hub.Register()

	// Fill the channel's buffer (capacity 8) so the next send would block
	// if Broadcast used a blocking send instead of select+default.
	for i := 0; i < 8; i++ {
		hub.Broadcast([]byte("fill"))
	}

	done := make(chan struct{})
	go func() {
		hub.Broadcast([]byte("overflow"))
		close(done)
	}()

	select {
	case <-done:
		// Broadcast returned without blocking — correct.
	case <-time.After(1 * time.Second):
		t.Fatal("Broadcast blocked on a full channel instead of skipping it")
	}

	// The dropped "overflow" message never displaced anything already
	// buffered — confirm the channel still yields "fill", not "overflow".
	if got := <-ch; string(got) != "fill" {
		t.Errorf("expected buffered message to still be %q, got %q", "fill", got)
	}
}

func TestHubUnregisterStopsFutureDeliveries(t *testing.T) {
	hub := NewHub()
	ch := hub.Register()
	hub.Unregister(ch)

	hub.Broadcast([]byte("should not arrive"))

	// The channel was closed by Unregister, so reading from it returns
	// the zero value immediately with ok == false, not a real message.
	got, ok := <-ch
	if ok {
		t.Errorf("expected channel to be closed after Unregister, got message %q", got)
	}
}
