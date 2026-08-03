package main

import "sync"

// Hub tracks every dashboard browser currently connected over WebSocket
// and broadcasts status snapshots to all of them. A single gorilla
// websocket.Conn isn't safe for concurrent writes, so each client gets its
// own buffered outbound channel — the Hub only ever pushes onto that
// channel; a dedicated writer goroutine (started by wsHandler) drains it
// and does the actual conn.WriteMessage calls. Hub itself knows nothing
// about websocket.Conn, which keeps it trivially testable with plain
// channels.
type Hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[chan []byte]struct{}),
	}
}

// Register creates and returns a new outbound channel for a client. The
// caller (a writer goroutine) drains this channel and is responsible for
// calling Unregister with the same channel when the client disconnects.
func (h *Hub) Register() chan []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan []byte, 8)
	h.clients[ch] = struct{}{}
	return ch
}

// Unregister removes a client's channel from the hub and closes it,
// signaling its writer goroutine to stop.
func (h *Hub) Unregister(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
}

// Broadcast pushes payload to every registered client's outbound channel.
// A client whose channel is full (a slow or stuck reader) is skipped for
// this broadcast rather than blocking delivery to everyone else.
func (h *Hub) Broadcast(payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- payload:
		default:
		}
	}
}
