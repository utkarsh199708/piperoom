package main

import (
	"errors"
	"testing"
)

// TestCheckListenAddr pins the listen guard: loopback addresses start with no
// flag; any other address needs --allow-remote, and with it the caller gets
// remote=true so it can log the dev-auth warning.
func TestCheckListenAddr(t *testing.T) {
	cases := []struct {
		name        string
		addr        string
		allowRemote bool
		wantRemote  bool
		wantErr     bool
	}{
		{name: "ipv4 loopback", addr: "127.0.0.1:8090"},
		{name: "ipv6 loopback", addr: "[::1]:8090"},
		{name: "localhost", addr: "localhost:8090"},
		{name: "localhost upper case", addr: "LocalHost:8090"},
		{name: "loopback with flag", addr: "127.0.0.1:8090", allowRemote: true},
		{name: "empty host means all interfaces", addr: ":8090", wantErr: true},
		{name: "all ipv4 interfaces", addr: "0.0.0.0:8090", wantErr: true},
		{name: "all ipv6 interfaces", addr: "[::]:8090", wantErr: true},
		{name: "lan address", addr: "192.168.1.5:8090", wantErr: true},
		{name: "remote host name", addr: "piperoom.example.com:8090", wantErr: true},
		{name: "remote with flag", addr: "0.0.0.0:8090", allowRemote: true, wantRemote: true},
		{name: "lan with flag", addr: "192.168.1.5:8090", allowRemote: true, wantRemote: true},
		{name: "no port", addr: "127.0.0.1", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			remote, err := checkListenAddr(tc.addr, tc.allowRemote)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("checkListenAddr(%q, %v): want error, got remote=%v", tc.addr, tc.allowRemote, remote)
				}
				return
			}
			if err != nil {
				t.Fatalf("checkListenAddr(%q, %v): unexpected error: %v", tc.addr, tc.allowRemote, err)
			}
			if remote != tc.wantRemote {
				t.Fatalf("checkListenAddr(%q, %v): remote=%v, want %v", tc.addr, tc.allowRemote, remote, tc.wantRemote)
			}
		})
	}
}

// TestCheckListenAddrRefusalIsTyped checks that a refused non-loopback address
// is matchable with errors.Is, so callers need not compare message text.
func TestCheckListenAddrRefusalIsTyped(t *testing.T) {
	_, err := checkListenAddr("0.0.0.0:8090", false)
	if !errors.Is(err, errNonLoopback) {
		t.Fatalf("refusal: want errNonLoopback, got %v", err)
	}
	_, err = checkListenAddr("not an address", false)
	if err == nil || errors.Is(err, errNonLoopback) {
		t.Fatalf("malformed address: want a parse error that is not errNonLoopback, got %v", err)
	}
}
