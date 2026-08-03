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
	metrics   map[string]*ServiceMetrics
	hub       *Hub
}

func NewSupervisor() *Supervisor {
	return &Supervisor{
		services:  make(map[string]time.Time),
		killChans: make(map[string]chan struct{}),
		status:    make(map[string]string),
		metrics:   make(map[string]*ServiceMetrics),
	}
}

// SetHub attaches a Hub that SetStatus will broadcast to on every call. A
// Supervisor with no Hub attached (the zero value, nil) skips broadcasting
// entirely — SetStatus still updates internal state as normal.
func (s *Supervisor) SetHub(hub *Hub) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hub = hub
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
// e.g. "starting", "running", "crashed", "exited", "killed-stale". If a
// Hub is attached (see SetHub), it also broadcasts the updated status
// snapshot to every connected dashboard — the broadcast happens after the
// lock is released, so a slow or stuck client can never hold up other
// goroutines trying to update Supervisor state.
func (s *Supervisor) SetStatus(name, status string) {
	s.mu.Lock()
	s.status[name] = status
	hub := s.hub
	s.mu.Unlock()

	if hub != nil {
		if payload, err := json.Marshal(s.Statuses()); err == nil {
			hub.Broadcast(payload)
		}
	}
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
	killCh := make(chan struct{}, 1) // buffered size 1: room for exactly one pending restart request
	if tracksHeartbeat {
		sup.RegisterKillChan(serviceName, killCh)
	}

	restarts := 0
	// downSince is set the moment a running process stops (crash, clean
	// exit, or stale-kill) and read back when the next attempt actually
	// reaches "running" again, to compute how long that incident's
	// downtime lasted. It stays zero before the service's very first
	// start, since that's not a recovery from anything.
	var downSince time.Time

	for {
		sup.SetStatus(serviceName, "starting")

		if tracksHeartbeat {
			// Reset this service's clock the moment we (re)start it, giving
			// it a full grace period to send its own first heartbeat before
			// we start checking it for staleness again. Without this, a
			// freshly restarted process could get killed again instantly —
			// before it's even had a chance to prove it's alive.
			sup.Heartbeat(serviceName)
		}

		start := time.Now()
		log.Printf("[supervise:%s] starting (attempt #%d)\n", serviceName, restarts+1)

		cmd := exec.Command(name, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// cmd.Start() launches the process but does NOT wait for it — this
		// is different from stage 2's cmd.Run(), which did both at once.
		// We need Start() alone here because we want to watch for TWO
		// possible things at once (exit or kill signal), which means we
		// can't let Run() block us on just one of them.
		if err := cmd.Start(); err != nil {
			log.Printf("[supervise:%s] failed to start: %v\n", serviceName, err)
			restarts++
			time.Sleep(1 * time.Second)
			continue
		}

		if !downSince.IsZero() {
			sup.RecordRestart(serviceName, time.Since(downSince))
			downSince = time.Time{}
		}
		sup.SetStatus(serviceName, "running")

		// cmd.Wait() blocks until the process exits, so we run it in its
		// own goroutine and have it report the result on "done" — this
		// frees up the current goroutine to simultaneously watch killCh.
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
				// Reset the clock right now, the instant we consume this
				// signal — not one loop iteration later. Without this,
				// there's a real gap between "we decided to kill" and
				// "the loop gets back around to resetting the timestamp,"
				// and if the checker's ticker fires again during that
				// gap, it still sees the old stale timestamp and queues
				// up a second kill — which lands on the process we just
				// restarted, within milliseconds, before it had any real
				// chance to prove itself.
				sup.Heartbeat(serviceName)
			}
			sup.SetStatus(serviceName, "killed-stale")
			cmd.Process.Kill() // it's still running — force it to stop
			<-done             // wait for Wait() to actually finish, so we don't leave a zombie process behind
			reason = "killed — went silent (no heartbeat received in time)"
		}

		uptime := time.Since(start)
		log.Printf("[supervise:%s] stopped after %v — %s — restarting\n", serviceName, uptime, reason)

		downSince = time.Now()
		restarts++
		time.Sleep(1 * time.Second)
	}
}

// metricsHandler returns an http.HandlerFunc that serves each service's
// incident history as JSON, e.g.
// {"hanging-worker": {"restart_count": 3, "total_downtime_ns": ..., "last_downtime_ns": ...}}
func metricsHandler(sup *Supervisor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sup.Metrics())
	}
}

// statusHandler returns an http.HandlerFunc that serves the Supervisor's
// current status snapshot as JSON, e.g. {"hanging-worker": "running"}.
func statusHandler(sup *Supervisor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sup.Statuses())
	}
}

const dashboardHTML = `<!DOCTYPE html>
<html>
<head>
<title>Watchdog Dashboard</title>
<style>
  body {
    background: #000;
    color: #fff;
    font-family: monospace;
    padding: 2rem;
  }
  h1 {
    font-weight: normal;
  }
  #tiles {
    display: flex;
    gap: 1rem;
    flex-wrap: wrap;
  }
  .tile {
    border: 1px solid #fff;
    padding: 1rem 2rem;
    min-width: 150px;
    text-align: center;
    white-space: pre;
  }
  .tile.running {
    background: #0a0;
    color: #000;
  }
  .tile.down {
    background: #a00;
    color: #fff;
  }
  #metrics {
    margin-top: 2rem;
  }
  #metrics table {
    border-collapse: collapse;
  }
  #metrics th, #metrics td {
    border: 1px solid #fff;
    padding: 0.5rem 1rem;
    text-align: left;
  }
</style>
</head>
<body>
<h1>Watchdog Dashboard</h1>
<div id="tiles"></div>
<div id="metrics"></div>
<script>
function fetchStatus() {
  fetch('/status')
    .then(function(res) { return res.json(); })
    .then(renderTiles)
    .catch(function(err) { console.log('status fetch failed:', err); });
}

function renderTiles(statuses) {
  var container = document.getElementById('tiles');
  container.innerHTML = '';
  for (var name in statuses) {
    var status = statuses[name];
    var tile = document.createElement('div');
    tile.className = 'tile ' + (status === 'running' ? 'running' : 'down');
    tile.textContent = name + '\n' + status;
    container.appendChild(tile);
  }
}

// Live push: the server sends the full status snapshot the moment it
// changes, so tiles update instantly instead of waiting for the next
// poll. The setInterval polling loop below keeps running regardless, as
// a fallback if the WebSocket connection ever drops or can't connect.
var wsProtocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
var ws = new WebSocket(wsProtocol + '//' + location.host + '/ws');
ws.onmessage = function(event) {
  renderTiles(JSON.parse(event.data));
};
ws.onerror = function(err) {
  console.log('websocket error, falling back to polling:', err);
};

function formatDuration(ns) {
  var seconds = ns / 1e9;
  if (seconds < 1) {
    return Math.round(ns / 1e6) + 'ms';
  }
  return seconds.toFixed(1) + 's';
}

function fetchMetrics() {
  fetch('/metrics')
    .then(function(res) { return res.json(); })
    .then(renderMetrics)
    .catch(function(err) { console.log('metrics fetch failed:', err); });
}

function renderMetrics(metrics) {
  var container = document.getElementById('metrics');
  var names = Object.keys(metrics);
  if (names.length === 0) {
    container.innerHTML = '';
    return;
  }
  var html = '<table><tr><th>Service</th><th>Restarts</th><th>Last downtime</th><th>Total downtime</th></tr>';
  names.forEach(function(name) {
    var m = metrics[name];
    html += '<tr><td>' + name + '</td><td>' + m.restart_count + '</td><td>' +
      formatDuration(m.last_downtime_ns) + '</td><td>' +
      formatDuration(m.total_downtime_ns) + '</td></tr>';
  });
  html += '</table>';
  container.innerHTML = html;
}

fetchStatus();
fetchMetrics();
setInterval(fetchStatus, 1000);
setInterval(fetchMetrics, 1000);
</script>
</body>
</html>
`

// dashboardHandler serves the static dashboard page. All live data comes
// from client-side polling of /status — this handler itself never touches
// the Supervisor.
func dashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, dashboardHTML)
}

// routes builds the HTTP mux for the supervisor's endpoints: /heartbeat,
// /status, /metrics, /dashboard, and /ws. Extracted out of main() so tests can
// exercise real routing (e.g. via httptest.NewServer) instead of calling
// handler functions directly, which would bypass the mux entirely and
// miss a mismatch between a registered path and what a client actually
// requests.
func routes(sup *Supervisor, hub *Hub) *http.ServeMux {
	mux := http.NewServeMux()

	// --- HTTP handler: services POST here to say "I'm alive" ---
	mux.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("/status", statusHandler(sup))
	mux.HandleFunc("/metrics", metricsHandler(sup))
	mux.HandleFunc("/dashboard", dashboardHandler)
	mux.HandleFunc("/ws", wsHandler(sup, hub))

	return mux
}

func main() {
	sup := NewSupervisor()

	hub := NewHub()
	sup.SetHub(hub)

	mux := routes(sup, hub)

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
	log.Fatal(http.ListenAndServe(":8080", mux))
}