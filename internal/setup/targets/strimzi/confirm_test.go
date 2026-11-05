package strimzi

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/setup/protocol"
)

const confirmPrefix, pairingName = "piperoom-setup-room1-confirm", "piperoom-setup-room1-pairing"

func confirmSecret(fp string, created time.Time) map[string]any {
	return map[string]any{
		"kind":     "Secret",
		"metadata": map[string]any{"name": confirmPrefix, "creationTimestamp": created.UTC().Format(time.RFC3339)},
		"data":     map[string]any{"fingerprint": base64.StdEncoding.EncodeToString([]byte(fp))},
	}
}

func newConfirmer(h *harness, now time.Time) *SecretConfirmer {
	h.api.allow(coreObjectPath(testNS, "secrets", confirmPrefix))
	h.api.put(coreObjectPath(testNS, "secrets", pairingName), map[string]any{"kind": "Secret", "data": map[string]any{"code": "Q0FGRQ=="}})
	return &SecretConfirmer{API: h.d.api, Namespace: testNS, Prefix: confirmPrefix, Pairing: pairingName,
		Now: func() time.Time { return now }, Poll: time.Millisecond, Timeout: 2 * time.Second}
}

func TestSecretConfirm(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	welcome := &protocol.Welcome{Fingerprint: "a1b2c3d4e5f6"}
	cases := []struct {
		name    string
		before  map[string]any // a confirm Secret present before the Welcome
		after   map[string]any // created once the block is shown
		ok      bool
		errWant string
	}{
		{"match", nil, confirmSecret("a1b2c3d4e5f6\n", now.Add(time.Second)), true, ""},
		{"skew allowance", nil, confirmSecret("a1b2c3d4e5f6", now.Add(-20*time.Second)), true, ""},
		{"mismatch", nil, confirmSecret("ffffffffffff", now), false, "does not match"},
		{"created early", nil, confirmSecret("a1b2c3d4e5f6", now.Add(-time.Minute)), false, "created before"},
		{"pre-existing", confirmSecret("a1b2c3d4e5f6", now), nil, false, "existed before"},
		{"timeout", nil, nil, false, "within"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, "", nil)
			c := newConfirmer(h, now)
			if tc.name == "timeout" {
				c.Timeout = 20 * time.Millisecond
			}
			path := coreObjectPath(testNS, "secrets", confirmPrefix)
			if tc.before != nil {
				h.api.put(path, tc.before)
			}
			shown := false
			ok, err := c.Confirm(context.Background(), welcome, func() {
				shown = true
				if tc.after != nil {
					h.api.put(path, tc.after)
				}
			})
			if ok != tc.ok {
				t.Fatalf("ok = %v, err = %v", ok, err)
			}
			if tc.errWant != "" && (!errors.Is(err, ErrConfirmRefused) || !strings.Contains(err.Error(), tc.errWant)) {
				t.Fatalf("err = %v, want ErrConfirmRefused containing %q", err, tc.errWant)
			}
			if tc.ok && err != nil {
				t.Fatal(err)
			}
			if tc.before != nil && shown {
				t.Error("the Welcome block was shown despite a pre-existing confirm Secret")
			}
			if h.api.get(path) != nil {
				t.Error("the confirm Secret was not deleted")
			}
			if h.api.get(coreObjectPath(testNS, "secrets", pairingName)) != nil {
				t.Error("the pairing Secret was not deleted")
			}
		})
	}
}

func TestSecretConfirmNeedsFingerprint(t *testing.T) {
	h := newHarness(t, "", nil)
	c := newConfirmer(h, time.Now())
	if _, err := c.Confirm(context.Background(), &protocol.Welcome{}, func() {}); !errors.Is(err, ErrConfirmRefused) {
		t.Fatalf("err = %v", err)
	}
}
