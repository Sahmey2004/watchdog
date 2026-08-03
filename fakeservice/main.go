package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"time"
)

// This stands in for a real service — like your Echo bot — that reports
// its own health. It sends a few heartbeats to prove it's alive, then
// deliberately goes silent WITHOUT crashing or exiting, simulating a
// hang: the process is still technically "running," it's just stopped
// doing anything useful. This is the exact scenario that only shows up
// via heartbeats, not via watching whether the process exited.
func main() {
	name := os.Args[1]
	fmt.Printf("[%s] starting up\n", name)

	for i := 1; i <= 3; i++ {
		body := []byte(fmt.Sprintf(`{"name":"%s"}`, name))
		http.Post("http://localhost:8080/heartbeat", "application/json", bytes.NewReader(body))
		fmt.Printf("[%s] sent heartbeat #%d\n", name, i)
		time.Sleep(1 * time.Second)
	}

	fmt.Printf("[%s] going silent now (simulating a hang, not a crash)\n", name)
	time.Sleep(1 * time.Hour) // still "running" — just never does anything else again
}
