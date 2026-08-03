package main

import "time"

// ServiceMetrics is the incident history for one supervised service:
// how many times it's been restarted, and how much time it has spent
// down in total and during its most recent incident. json.Marshal turns
// the two time.Duration fields into plain nanosecond integers, which the
// dashboard's JS formats into human-readable text.
type ServiceMetrics struct {
	RestartCount  int           `json:"restart_count"`
	TotalDowntime time.Duration `json:"total_downtime_ns"`
	LastDowntime  time.Duration `json:"last_downtime_ns"`
}

// RecordRestart logs one incident for a service: it was down for
// `downtime` before this restart succeeded. Called by Supervise() the
// moment a restarted process reaches "running" again — never on the very
// first start of a service, since that's not a recovery from anything.
func (s *Supervisor) RecordRestart(name string, downtime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.metrics[name]
	if m == nil {
		m = &ServiceMetrics{}
		s.metrics[name] = m
	}
	m.RestartCount++
	m.TotalDowntime += downtime
	m.LastDowntime = downtime
}

// Metrics returns a snapshot copy of every known service's incident
// history — a copy, not the live map, for the same race-safety reason as
// Statuses().
func (s *Supervisor) Metrics() map[string]ServiceMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]ServiceMetrics, len(s.metrics))
	for name, m := range s.metrics {
		out[name] = *m
	}
	return out
}
