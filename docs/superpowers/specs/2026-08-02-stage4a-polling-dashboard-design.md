# Stage 4a: Polling Status Dashboard — Design

## Background

Stages 1–3 (Supervisor, heartbeat/staleness detection, process supervision
with crash + hang recovery) are complete and working, but the only visible
output is scrolling log lines in a terminal. The original Stage 4 plan
(`watchdog-project-summary.md`) called for a real-time dashboard pushed via
WebSockets. Given the project's teaching goals (Go beginner, incremental
stages, 1–2 new concepts at a time), Stage 4 is split into two sub-stages:

- **Stage 4a (this spec)**: a simple HTML dashboard that polls a JSON status
  endpoint every second. New concepts: serving HTML from Go, a JSON status
  endpoint, plain JS `fetch`/`setInterval`.
- **Stage 4b (future)**: swap polling for WebSocket push. New concepts:
  connection upgrade, broadcasting to multiple clients.

## Goal

Show each supervised service (`flaky-crasher`, `hanging-worker`) as a tile
that goes red when the service is down/stale and green when it's running,
updating within ~1s, visible at `/dashboard` in a browser.

## Problem: no existing "current status" state

`Supervisor` currently only tracks `services map[string]time.Time` (last
heartbeat), populated lazily — only when `Heartbeat()` is called. Because
`flaky-crasher` runs with `tracksHeartbeat=false`, it **never** calls
`Heartbeat()` and therefore never appears in `services` at all. There is no
existing signal for "is this process currently running" independent of
heartbeat staleness, and no registry of "all known service names."

Deriving dashboard status purely from heartbeat staleness would make
`flaky-crasher` look permanently red/missing even while healthy — a poor
demo of the crash-detection path this project exists to showcase.

## Design

### Supervisor: explicit status state

Add a new field to `Supervisor`:

```go
status map[string]string // guarded by the existing mu
```

Two new methods, following the existing `Heartbeat`/`CheckStale` mutex
pattern:

- `SetStatus(name, status string)` — mutex-protected setter.
- `Statuses() map[string]string` — mutex-protected getter that returns a
  **copy** of the map (never the live map), so callers outside the lock
  can't race with concurrent writes.

`Supervise()` calls `sup.SetStatus(serviceName, ...)` at each state
transition it already detects:

| Point in the existing loop                     | Status         |
|--------------------------------------------------|----------------|
| Top of loop, before `cmd.Start()`                 | `"starting"`   |
| Immediately after `cmd.Start()` succeeds          | `"running"`    |
| `done` fires with non-nil error (crash)           | `"crashed"`    |
| `done` fires with nil error (clean exit)          | `"exited"`     |
| `killCh` fires (killed for going stale)           | `"killed-stale"` |

This makes status *pushed* by the goroutine that already knows what's
happening, rather than *derived* after the fact — more accurate (e.g.
`"killed-stale"` is distinguishable from `"crashed"`, a distinction the
current code already has in its log strings but exposes nowhere else) and
uniform across both heartbeat-tracked and non-heartbeat-tracked services.

No changes to the existing `Heartbeat`/`CheckStale`/staleness-checker path.

### New HTTP endpoints

- `GET /status` — returns `sup.Statuses()` as JSON, e.g.:
  ```json
  {"flaky-crasher": "crashed", "hanging-worker": "running"}
  ```
  Both demo services are registered via `Supervise()` before `main()` starts
  serving, so this is never empty once the server is up.

- `GET /dashboard` — returns an inline HTML page (a Go string constant,
  `dashboardHTML`, written to `w` via `fmt.Fprint`). No separate file, no
  `html/template` — just a plain string, consistent with "no new concepts
  beyond what's already known" for this stage.

### Frontend

Plain HTML + inline `<style>` + inline `<script>`, no framework, no build
step:

- Page style: black background, white text, flat colored tiles for status
  (green = `"running"`, red = anything else). No gradients.
- One `<div>` tile per service name, created/updated from the `/status`
  JSON response.
- `setInterval(fetchStatus, 1000)` calls `fetch('/status')`, parses JSON,
  updates tile text/color per service name.
- Fetch failures (e.g. server briefly restarting) are logged to console and
  swallowed — the next poll just tries again. No user-facing error state.

### Data flow

```
Supervise() goroutine  --SetStatus()-->  Supervisor.status map
                                              |
                                      GET /status (mutex-protected copy)
                                              |
                                    browser fetch() every 1s
                                              |
                                      JS repaints tiles
```

This is independent of the existing heartbeat/staleness-checker flow, which
continues to drive restarts exactly as it does today.

## Testing

Extend `main_test.go` with a table test for `SetStatus`/`Statuses()`:

- Set a few `(name, status)` pairs, verify `Statuses()` returns them.
- Verify `Statuses()` returns an independent copy: mutate the returned map
  after the call, confirm `Supervisor`'s internal state is unaffected.

No automated test for the HTML/JS — verified by hand in the browser
(existing "run it, show me real output" workflow): start the server, open
`/dashboard`, confirm both tiles render, kill `hanging-worker`'s process
externally and confirm the tile flips red then recovers.

## Out of scope (deferred to later stages)

- WebSocket push (Stage 4b).
- Restart counts / incident history / downtime metrics (roadmap item after
  the dashboard).
- Any dashboard auth — this is a local demo project, not exposed publicly.
