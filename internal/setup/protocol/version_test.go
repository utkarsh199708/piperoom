package protocol_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/setup/protocol"
)

func v(major, minor int) protocol.Version { return protocol.Version{Major: major, Minor: minor} }

// The compatibility rule of the package comment, both sides, as one table:
// same major, and each side's minor at most one behind the other's.
func TestCompatibilityRule(t *testing.T) {
	cases := []struct {
		name           string
		server, runner protocol.Version
		serverOK       bool // ServerAccepts(server, runner)
		runnerOK       bool // RunnerAccepts(runner, server)
		negotiated     protocol.Version
	}{
		{"same version", v(1, 1), v(1, 1), true, true, v(1, 1)},
		{"runner one minor behind pairs", v(1, 1), v(1, 0), true, true, v(1, 0)},
		{"runner one minor behind, later minors", v(1, 5), v(1, 4), true, true, v(1, 4)},
		{"runner two minors behind: the server refuses", v(1, 5), v(1, 3), false, true, protocol.Version{}},
		{"runner one minor ahead pairs", v(1, 1), v(1, 2), true, true, v(1, 1)},
		{"runner two minors ahead: the runner refuses", v(1, 1), v(1, 3), true, false, protocol.Version{}},
		{"both at 1.0", v(1, 0), v(1, 0), true, true, v(1, 0)},
		{"older major runner", v(2, 0), v(1, 9), false, false, protocol.Version{}},
		{"newer major runner", v(1, 9), v(2, 0), false, false, protocol.Version{}},
		{"a runner that sent no version never pairs", v(1, 1), v(0, 0), false, false, protocol.Version{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			serr := protocol.ServerAccepts(c.server, c.runner)
			rerr := protocol.RunnerAccepts(c.runner, c.server)
			if (serr == nil) != c.serverOK {
				t.Errorf("ServerAccepts(%s, %s) = %v, want ok %v", c.server, c.runner, serr, c.serverOK)
			}
			if (rerr == nil) != c.runnerOK {
				t.Errorf("RunnerAccepts(%s, %s) = %v, want ok %v", c.runner, c.server, rerr, c.runnerOK)
			}
			for _, err := range []error{serr, rerr} {
				if err != nil && !errors.Is(err, protocol.ErrProtocolVersion) {
					t.Errorf("refusal %v does not wrap ErrProtocolVersion", err)
				}
			}
			if serr == nil && rerr == nil {
				if got := protocol.Negotiate(c.server, c.runner); got != c.negotiated {
					t.Errorf("Negotiate = %s, want %s", got, c.negotiated)
				}
			}
		})
	}
}

// The refusal names both versions and fits a WebSocket close reason (123
// bytes) once the sentinel's prefix is cut.
func TestRefusalText(t *testing.T) {
	err := protocol.ServerAccepts(v(1, 3), v(1, 0))
	reason := strings.TrimPrefix(err.Error(), protocol.ErrProtocolVersion.Error()+": ")
	if reason != "server 1.3 accepts runner 1.2 or later, runner is 1.0" {
		t.Errorf("reason = %q", reason)
	}
	if len(reason) > 123 {
		t.Errorf("reason is %d bytes", len(reason))
	}
	if got := protocol.RunnerAccepts(v(1, 3), v(1, 1)).Error(); !strings.Contains(got, "runner 1.3 needs server 1.2 or later, server is 1.1") {
		t.Errorf("runner refusal = %q", got)
	}
}

// This build pairs with itself and with a runner one minor behind.
func TestCurrent(t *testing.T) {
	cur := protocol.Current()
	if cur != v(protocol.CurrentMajor, protocol.CurrentMinor) || cur.String() != "1.1" {
		t.Fatalf("Current = %s", cur)
	}
	behind := v(cur.Major, cur.Minor-1)
	if err := protocol.ServerAccepts(cur, behind); err != nil {
		t.Errorf("a runner one minor behind: %v", err)
	}
	if err := protocol.RunnerAccepts(behind, cur); err != nil {
		t.Errorf("that runner refuses this server: %v", err)
	}
}

func TestHasAndFrameSince(t *testing.T) {
	if !v(1, 1).Has(0) || !v(1, 1).Has(1) || v(1, 1).Has(2) || !v(1, 0).Has(0) || v(1, 0).Has(1) {
		t.Error("Has")
	}
	for _, ft := range []protocol.FrameType{
		protocol.FramePair, protocol.FrameWelcome, protocol.FrameHello, protocol.FrameToken, protocol.FrameOp,
		protocol.FrameResult, protocol.FrameRefused, protocol.FrameResume, protocol.FrameCancel,
		protocol.FrameRegisterCredentials, protocol.FrameCredentials, protocol.FrameSecret,
		protocol.FramePing, protocol.FramePong,
	} {
		since, known := protocol.FrameSince(ft)
		if !known || !v(1, 0).Has(since) {
			t.Errorf("%s: since %d known %v; every U0 frame type is 1.0", ft, since, known)
		}
	}
	if _, known := protocol.FrameSince("exec"); known {
		t.Error("an unknown type is known")
	}
}

// A 1.0 runner's frames (the U0 shapes) decode unchanged under 1.1 and pair;
// a 1.1 Pair, Hello and Welcome carry the minor.
func TestWireBackwardCompatible(t *testing.T) {
	var f protocol.Frame
	if err := json.Unmarshal([]byte(`{"type":"pair","pair":{"code":"ABCDEFGH"}}`), &f); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.Pair.Version(); ok {
		t.Error("a 1.0 pair (no version) reported one; the server must defer the check to hello")
	}
	if err := json.Unmarshal([]byte(`{"type":"hello","hello":{"os":"windows","arch":"amd64","label":"l","tools":{},"project_dir":"C:\\p","catalogue_version":"1","protocol_version":1}}`), &f); err != nil {
		t.Fatal(err)
	}
	if got := f.Hello.Version(); got != v(1, 0) {
		t.Errorf("1.0 hello = %s", got)
	}
	if err := protocol.ServerAccepts(protocol.Current(), f.Hello.Version()); err != nil {
		t.Errorf("a 1.0 runner does not pair with this server: %v", err)
	}
	var w protocol.Welcome
	if err := json.Unmarshal([]byte(`{"room_id":"r","title":"t","pipeline_id":"p","creator":{"kind":"human","id":"a"},"protocol_version":1}`), &w); err != nil {
		t.Fatal(err)
	}
	if w.Version() != v(1, 0) {
		t.Errorf("1.0 welcome = %s", w.Version())
	}

	raw, err := json.Marshal(protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{Code: "ABCDEFGH", ProtocolVersion: 1, ProtocolMinor: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"pair","pair":{"code":"ABCDEFGH","protocol_version":1,"protocol_minor":1}}`; string(raw) != want {
		t.Errorf("1.1 pair = %s", raw)
	}
	var p protocol.Frame
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if got, ok := p.Pair.Version(); !ok || got != v(1, 1) {
		t.Errorf("1.1 pair version = %s %v", got, ok)
	}
	raw, err = json.Marshal(protocol.Welcome{RoomID: "r", ProtocolVersion: 1, ProtocolMinor: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"protocol_version":1,"protocol_minor":1`) {
		t.Errorf("welcome = %s", raw)
	}
	raw, err = json.Marshal(protocol.Hello{ProtocolVersion: 1, ProtocolMinor: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"protocol_version":1,"protocol_minor":1`) {
		t.Errorf("hello = %s", raw)
	}
}

// A newer peer's unknown fields are ignored and its unknown frame types are
// dropped, never acted on; a malformed known frame closes the socket.
func TestFrameCheck(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want error
	}{
		{"op", `{"type":"op","op":{"id":"1","op":"env_detect","params":{},"tier":"T0","class":"local","deadline":"2026-10-19T10:00:00Z"}}`, nil},
		{"op with fields from a newer minor", `{"type":"op","op":{"id":"1","op":"env_detect","params":{},"tier":"T0","class":"local","deadline":"2026-10-19T10:00:00Z","priority":"high"},"trace":"x"}`, nil},
		{"ping", `{"type":"ping"}`, nil},
		{"pong", `{"type":"pong"}`, nil},
		{"pair", `{"type":"pair","pair":{"token":"t"}}`, nil},
		{"unknown type from a newer minor", `{"type":"exec","exec":{"argv":["rm","-rf","/"]}}`, protocol.ErrUnknownFrame},
		{"unknown type carrying a known payload", `{"type":"exec","op":{"id":"1","op":"compose_up"}}`, protocol.ErrUnknownFrame},
		{"empty type", `{"op":{"id":"1"}}`, protocol.ErrUnknownFrame},
		{"op without its payload", `{"type":"op"}`, protocol.ErrMalformedFrame},
		{"op with a second payload", `{"type":"op","op":{"id":"1"},"secret":{"name":"a","value":"bbbbbb"}}`, protocol.ErrMalformedFrame},
		{"ping with a payload", `{"type":"ping","op":{"id":"1"}}`, protocol.ErrMalformedFrame},
		{"result typed as op", `{"type":"op","result":{"id":"1"}}`, protocol.ErrMalformedFrame},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var f protocol.Frame
			if err := json.Unmarshal([]byte(c.raw), &f); err != nil {
				t.Fatalf("decode: %v (an unknown field must never fail decoding)", err)
			}
			err := f.Check()
			if (c.want == nil) != (err == nil) || (c.want != nil && !errors.Is(err, c.want)) {
				t.Errorf("Check = %v, want %v", err, c.want)
			}
		})
	}
}
