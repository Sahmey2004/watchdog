package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Supervisor holds the shared state: which services exist and when each
// one last checked in, plus a way to signal each running process to
// restart on demand. Multiple goroutines touch this at the same time (the
// HTTP handler, the checker, and each supervise loop), so it needs a Mutex
// to avoid a "concurrent map write" crash.
type Supervisor struct {
	mu        sync.Mutex
	services  map[string]time.Time
	killChans map[string]chan struct{}
	status    map[string]string
}

func NewSupervisor() *Supervisor {
	return &Supervisor{
		services:  make(map[string]time.Time),
		killChans: make(map[string]chan struct{}),
		status:    make(map[string]string),
	}
}

// RegisterKillChan lets a Supervise loop hand the Supervisor a channel it
// can send on to request "restart this service right now." Think of it as
// each supervised process leaving a doorbell for the checker to ring.
func (s *Supervisor) RegisterKillChan(name string, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.killChans[name] = ch
}

// TriggerRestart asks the named service's Supervise loop to kill and
// restart its process right now. "chan struct{}" is Go's idiom for "a
// channel that carries no real data — it's a pure signal, only its
// arrival matters." The select/default below makes the send non-blocking:
// if a restart request is already pending, this just does nothing instead
// of stacking up duplicate requests.
func (s *Supervisor) TriggerRestart(name string) {
	s.mu.Lock()
	ch, ok := s.killChans[name]
	s.mu.Unlock()
	if !ok {
		return // no supervise loop registered under this name — nothing to signal
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// SetStatus records the current lifecycle status of a supervised service,
// e.g. "starting", "running", "crashed", "exited", "killed-stale".
func (s *Supervisor) SetStatus(name, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[name] = status
}

// Statuses returns a snapshot copy of every known service's current
// status. It returns a copy, not the live map, so callers can read or
// mutate the result after the call without racing against concurrent
// writes from Supervise goroutines.
func (s *Supervisor) Statuses() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.status))
	for name, status := range s.status {
		out[name] = status
	}
	return out
}

// Heartbeat records that a service is alive right now.
// The receiver is (s *Supervisor) — a pointer receiver — so this method can
// actually modify the struct's fields, rather than working on a copy of it.
func (s *Supervisor) Heartbeat(name string) {
	s.mu.Lock()         // grab the lock before touching the map
	defer s.mu.Unlock() // "defer" schedules Unlock() to run when this
	// function returns, no matter how it returns — even if something
	// panics. Guarantees we never forget to release the lock.
	s.services[name] = time.Now()
}

// CheckStale returns the names of services that haven't sent a heartbeat
// within the given timeout.
func (s *Supervisor) CheckStale(timeout time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var stale []string
	for name, last := range s.services {
		if time.Since(last) > timeout {
			stale = append(stale, name)
		}
	}
	return stale
}

// Supervise starts a command as a real child process and keeps it alive
// forever, restarting it whenever it stops — whether it stops by crashing,
// exiting cleanly, or being killed because it went silent.
//
// tracksHeartbeat should be true only for services that actually send
// heartbeats to /heartbeat themselves. For those, Supervise registers a
// kill channel so the stale-checker can reach in and restart it on a hang
// — not just a crash.
func Supervise(sup *Supervisor, serviceName string, tracksHeartbeat bool, name string, args ...string) {
	killCh := make(chan struct{}, 1)
	if tracksHeartbeat {
		sup.RegisterKillChan(serviceName, killCh)
	}

	restarts := 0

	for {
		sup.SetStatus(serviceName, "starting")

		if tracksHeartbeat {
			sup.Heartbeat(serviceName)
		}

		start := time.Now()
		log.Printf("[supervise:%s] starting (attempt #%d)\n", serviceName, restarts+1)

		cmd := exec.Command(name, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Printf("[supervise:%s] failed to start: %v\n", serviceName, err)
			restarts++
			time.Sleep(1 * time.Second)
			continue
		}

		sup.SetStatus(serviceName, "running")

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		var reason string
		select {
		case err := <-done:
			if err != nil {
				reason = fmt.Sprintf("crashed: %v", err)
				sup.SetStatus(serviceName, "crashed")
			} else {
				reason = "exited cleanly"
				sup.SetStatus(serviceName, "exited")
			}
		case <-killCh:
			if tracksHeartbeat {
				sup.Heartbeat(serviceName)
			}
			sup.SetStatus(serviceName, "killed-stale")
			cmd.Process.Kill()
			<-done
			reason = "killed — went silent (no heartbeat received in time)"
		}

		uptime := time.Since(start)
		log.Printf("[supervise:%s] stopped after %v — %s — restarting\n", serviceName, uptime, reason)

		restarts++
		time.Sleep(1 * time.Second)
	}
}

func main() {
	sup := NewSupervisor()

	// --- HTTP handler: services POST here to say "I'm alive" ---
	http.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		sup.Heartbeat(payload.Name)
		fmt.Fprintf(w, "ok\n")
	})

	// --- Process supervision goroutines ---
	// Two demo services, showing the two failure modes this project cares
	// about:
	//   1. flaky-crasher: exits on its own (crash) — caught by Supervise's
	//      own "done" channel, tracksHeartbeat=false since it never
	//      reports its own health.
	//   2. hanging-worker: keeps running but goes silent (a hang) — only
	//      catchable via heartbeats, tracksHeartbeat=true so the checker
	//      can reach in and kill it.
	go Supervise(sup, "flaky-crasher", false, "sh", "-c", "sleep 3 && exit 1")
	go Supervise(sup, "hanging-worker", true, "./bin/fakeservice", "hanging-worker")

	// --- Background checker goroutine ---
	// "go func() { ... }()" launches this block as a separate goroutine —
	// it runs concurrently with the HTTP server below, sharing the same
	// Supervisor instance. Goroutines are cheap (a few KB each, managed by
	// the Go runtime, not the OS) which is why Go can spin up thousands of
	// them without breaking a sweat.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C { // fires every second
			stale := sup.CheckStale(3 * time.Second)
			for _, name := range stale {
				log.Printf("WARNING: %s has gone silent — triggering restart\n", name)
				sup.TriggerRestart(name)
			}
		}
	}()

	log.Println("Supervisor listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}