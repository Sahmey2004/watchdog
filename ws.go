package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// The dashboard is always served from the same origin as this server in
// this project (no separate frontend host), so the upgrader's default
// same-origin check is fine as-is — no CheckOrigin override needed.
var upgrader = websocket.Upgrader{}

// wsHandler upgrades the HTTP connection to a WebSocket, registers it with
// the Hub, and immediately sends the current status snapshot so a freshly
// opened dashboard doesn't have to wait for the next SetStatus event to
// see anything. It then runs two goroutines for the lifetime of the
// connection:
//   - a writer that drains the Hub-assigned channel and writes each
//     payload to the socket (the only goroutine allowed to write to this
//     conn, since gorilla's Conn isn't safe for concurrent writes)
//   - a reader that blocks on ReadMessage purely to detect the client
//     disconnecting (the dashboard never sends anything itself)
//
// Either goroutine noticing a broken connection unregisters the client;
// Hub.Unregister is safe to call twice (a no-op the second time), so it
// doesn't matter which goroutine gets there first.
func wsHandler(sup *Supervisor, hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("websocket upgrade failed: %v\n", err)
			return
		}

		ch := hub.Register()

		if payload, err := json.Marshal(sup.Statuses()); err == nil {
			select {
			case ch <- payload:
			default:
			}
		}

		go func() {
			defer conn.Close()
			for payload := range ch {
				if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
					hub.Unregister(ch)
					return
				}
			}
		}()

		go func() {
			defer hub.Unregister(ch)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
}
