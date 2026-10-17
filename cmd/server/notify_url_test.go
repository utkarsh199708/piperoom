package main

import (
	"testing"

	"piperoom/internal/config"
)

// S10: the page webhook's room link falls back to the loopback listen
// address; a public URL wins, and a non-loopback address is never guessed.
func TestNotifyBaseURL(t *testing.T) {
	cases := []struct {
		name, public, addr, want string
	}{
		{"public url wins", "https://piperoom.example.com", "127.0.0.1:8090", "https://piperoom.example.com"},
		{"loopback ipv4", "", "127.0.0.1:8090", "http://127.0.0.1:8090"},
		{"localhost", "", "localhost:8090", "http://localhost:8090"},
		{"loopback ipv6", "", "[::1]:8090", "http://[::1]:8090"},
		{"all interfaces", "", ":8090", ""},
		{"non-loopback", "", "10.0.0.5:8090", ""},
		{"wildcard", "", "0.0.0.0:8090", ""},
		{"garbage", "", "nope", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := notifyBaseURL(config.Config{PublicURL: c.public, HTTPAddr: c.addr}); got != c.want {
				t.Fatalf("notifyBaseURL = %q, want %q", got, c.want)
			}
		})
	}
}
