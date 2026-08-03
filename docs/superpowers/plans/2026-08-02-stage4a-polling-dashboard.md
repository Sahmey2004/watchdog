# Stage 4a: Polling Status Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add a `GET /dashboard` page that polls a new `GET /status` JSON endpoint every second and shows each supervised service as a black-background tile that goes green when running and red otherwise.

**Architecture:** `Supervisor` gains a mutex-protected `status map[string]string`, written directly by `Supervise()` at each lifecycle transition (starting/running/crashed/exited/killed-stale) it already detects. `/status` serves a JSON snapshot of that map; `/dashboard` serves a static HTML+CSS+JS page that polls `/status` client-side and repaints tiles. No change to the existing heartbeat/staleness-checker path.

**Tech Stack:** Go 1.22 standard library only (`net/http`, `encoding/json`, `net/http/httptest` for tests). No new dependencies, no frontend framework.

## Global Constraints

- Go 1.22, standard library only — no new entries in `go.mod`.
- Dashboard styling: black background, white text, flat colors for tile status (no gradients) — per `docs/superpowers/specs/2026-08-02-stage4a-polling-dashboard-design.md`.
- Follow the existing white-box test pattern in `main_test.go` (direct access to unexported `Supervisor` fields under `sup.mu`).
- `Statuses()` must return a copy of the internal map, never the live map (see design spec's race-safety rationale).

---

### Task 1: Supervisor status state (`SetStatus` / `Statuses`)

**Files:**
- Modify: `main.go:19-30` (`Supervisor` struct and `NewSupervisor`)
- Test: `main_test.go`

**Interfaces:**
- Produces: `func (s *Supervisor) SetStatus(name, status string)` — records current status for a service.
- Produces: `func (s *Supervisor) Statuses() map[string]string` — returns a snapshot copy of all known statuses.

- [x] **Step 1: Write the failing test**

Add to `main_test.go`:

```go
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
```

- [x] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestSetStatusAndStatuses -v`
Expected: FAIL — compile error, `sup.SetStatus` / `sup.status` undefined.

- [x] **Step 3: Write minimal implementation**

In `main.go`, add `status` to the struct and initialize it:

```go
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
```

Add the two new methods (place after `TriggerRestart`, before `Heartbeat`):

```go
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
```

- [x] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestSetStatusAndStatuses -v`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "Add Supervisor.SetStatus/Statuses for tracking live service status"
```

---

### Task 2: Wire status transitions into `Supervise()`

**Files:**
- Modify: `main.go:94-175` (`Supervise` function)
- Test: `main_test.go`

**Interfaces:**
- Consumes: `sup.SetStatus(name, status string)` from Task 1.
- Produces: `Supervise()` now calls `SetStatus` with one of `"starting"`, `"running"`, `"crashed"`, `"exited"`, `"killed-stale"` at each lifecycle point — later tasks (`/status` handler) rely on these exact string values.

- [x] **Step 1: Write the failing test**

Add to `main_test.go`:

```go
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
```

- [x] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestSuperviseSetsStatusTransitions -v`
Expected: FAIL — timeout, status never reaches `"exited"` because `Supervise` never calls `SetStatus`.

- [x] **Step 3: Write minimal implementation**

In `main.go`, modify `Supervise` (full replacement of the function body from the `for {` loop) to add `SetStatus` calls at each transition:

```go
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
```

(This only adds three `sup.SetStatus(...)` calls and keeps every existing line, comment, and behavior otherwise unchanged.)

- [x] **Step 4: Run test to verify it passes**

Run: `go test ./... -v`
Expected: PASS — all tests, including the new one and the existing `TestHeartbeatAndCheckStale` / `TestSetStatusAndStatuses`.

- [x] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "Wire SetStatus calls into Supervise lifecycle transitions"
```

---

### Task 3: `GET /status` JSON endpoint

**Files:**
- Modify: `main.go` (extract a named `statusHandler`, register it in `main()`)
- Test: `main_test.go`

**Interfaces:**
- Consumes: `sup.Statuses() map[string]string` from Task 1.
- Produces: `func statusHandler(sup *Supervisor) http.HandlerFunc` — later manual verification (Task 4) relies on this being registered at `/status` and returning `application/json`.

- [x] **Step 1: Write the failing test**

Add to `main_test.go` (add `"net/http"`, `"net/http/httptest"` to imports):

```go
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
```

`main_test.go` needs `"encoding/json"` too — check it's not already imported before adding.

- [x] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestStatusHandler -v`
Expected: FAIL — compile error, `statusHandler` undefined.

- [x] **Step 3: Write minimal implementation**

In `main.go`, add above `func main()`:

```go
// statusHandler returns an http.HandlerFunc that serves the Supervisor's
// current status snapshot as JSON, e.g. {"hanging-worker": "running"}.
func statusHandler(sup *Supervisor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sup.Statuses())
	}
}
```

In `main()`, register it alongside the existing `/heartbeat` handler:

```go
http.HandleFunc("/status", statusHandler(sup))
```

- [x] **Step 4: Run test to verify it passes**

Run: `go test ./... -v`
Expected: PASS — all tests.

- [x] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "Add GET /status JSON endpoint"
```

---

### Task 4: `GET /dashboard` page and end-to-end verification

**Files:**
- Modify: `main.go` (add `dashboardHTML` constant, `dashboardHandler`, register route)
- Test: `main_test.go`

**Interfaces:**
- Consumes: `/status` endpoint from Task 3 (referenced by URL string inside the served JS, not a Go-level dependency).
- Produces: `func dashboardHandler(w http.ResponseWriter, r *http.Request)` registered at `GET /dashboard`.

- [x] **Step 1: Write the failing test**

Add to `main_test.go` (add `"strings"` to imports):

```go
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
	for _, want := range []string{"/status", "setInterval", "background: #000", "color: #fff"} {
		if !strings.Contains(body, want) {
			t.Errorf("expected dashboard HTML to contain %q", want)
		}
	}
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestDashboardHandler -v`
Expected: FAIL — compile error, `dashboardHandler` undefined.

- [x] **Step 3: Write minimal implementation**

In `main.go`, add above `func main()`:

```go
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
</style>
</head>
<body>
<h1>Watchdog Dashboard</h1>
<div id="tiles"></div>
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

fetchStatus();
setInterval(fetchStatus, 1000);
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
```

In `main()`, register it:

```go
http.HandleFunc("/dashboard", dashboardHandler)
```

- [x] **Step 4: Run test to verify it passes**

Run: `go test ./... -v`
Expected: PASS — all tests.

- [x] **Step 5: Manual end-to-end verification**

```bash
go build -o bin/fakeservice ./fakeservice
go run main.go
```

In a browser, open `http://localhost:8080/dashboard`. Confirm:
- Black background, white text, no gradients.
- Two tiles: `hanging-worker` and `flaky-crasher`.
- `flaky-crasher` cycles red (crashed) → briefly green (running) → red, roughly every few seconds, matching its `sleep 3 && exit 1` cycle.
- `hanging-worker` shows green (running) most of the time, flips red (`killed-stale`) briefly around the 3s-heartbeat-timeout mark, then back to green after it restarts.

Stop the server with Ctrl+C when done observing.

- [x] **Step 6: Commit**

```bash
git add main.go main_test.go
git commit -m "Add GET /dashboard polling status page"
```
