package strimzi

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"piperoom/internal/setup/protocol"
)

// The in-cluster runner's pairing (docs/design/setup-strimzi.md §3.1, §3.2).
// A Job has no terminal and cannot create anything, so the person's "y" is a
// Secret they create with their own kubectl, carrying the fingerprint the
// Set up page shows; the runner only gets and deletes Secrets by name.

// ErrConfirmRefused is a confirmation the runner refused: a confirm Secret
// that existed before the Welcome, a wrong fingerprint, a Secret created
// before the Welcome was logged, or no Secret in time.
var ErrConfirmRefused = errors.New("strimzi: pairing confirmation refused")

// clockSkew is how far the pod's clock may run ahead of the API server's: a
// confirm Secret created more than this before the Welcome was logged is
// refused as pre-existing.
const clockSkew = 30 * time.Second

// namespaceFile is the Job's namespace from the projected token volume.
const namespaceFile = saDir + "/namespace"

// InClusterNamespace reads the Job's namespace from the mounted ServiceAccount
// files.
func InClusterNamespace() (string, error) {
	b, err := os.ReadFile(namespaceFile)
	if err != nil {
		return "", fmt.Errorf("strimzi: reading the namespace: %w", err)
	}
	ns := strings.TrimSpace(string(b))
	if !nameRE.MatchString(ns) {
		return "", fmt.Errorf("strimzi: the mounted namespace %q is not a namespace name", clipText(ns, 64))
	}
	return ns, nil
}

// SecretConfirmer confirms a pairing through Secrets named by the chart:
// Prefix ("piperoom-setup-<room>-confirm") is the confirmation, Pairing
// ("piperoom-setup-<room>-pairing") the code the Job read, deleted as soon as
// the Welcome arrived.
type SecretConfirmer struct {
	API       *API
	Namespace string
	Prefix    string
	Pairing   string
	// Now, Poll and Timeout default to time.Now, 2 s and protocol.CodeTTL.
	Now     func() time.Time
	Poll    time.Duration
	Timeout time.Duration
}

// Confirm runs §3.2: delete the pairing Secret, refuse a confirm Secret that
// exists already, call show (the runner logs the Welcome block with the
// fingerprint), then wait for the person's confirm Secret and accept it only
// if it carries the Welcome's fingerprint and was created after show. Every
// confirm Secret it sees is deleted.
func (c *SecretConfirmer) Confirm(ctx context.Context, w *protocol.Welcome, show func()) (bool, error) {
	now, poll, timeout := c.Now, c.Poll, c.Timeout
	if now == nil {
		now = time.Now
	}
	if poll <= 0 {
		poll = 2 * time.Second
	}
	if timeout <= 0 {
		timeout = protocol.CodeTTL
	}
	if w.Fingerprint == "" {
		return false, fmt.Errorf("%w: the server sent no fingerprint to confirm", ErrConfirmRefused)
	}
	if c.Pairing != "" {
		if err := c.API.Delete(ctx, coreObjectPath(c.Namespace, "secrets", c.Pairing)); err != nil && !errors.Is(err, ErrNotFound) {
			return false, fmt.Errorf("strimzi: deleting the pairing Secret: %w", err)
		}
	}
	path := coreObjectPath(c.Namespace, "secrets", c.Prefix)
	if _, err := c.API.Get(ctx, path); err == nil {
		_ = c.API.Delete(ctx, path)
		return false, fmt.Errorf("%w: a confirm Secret %s existed before the Welcome", ErrConfirmRefused, c.Prefix)
	} else if !errors.Is(err, ErrNotFound) {
		return false, fmt.Errorf("strimzi: reading the confirm Secret: %w", err)
	}
	show()
	logged := now()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-deadline.C:
			return false, fmt.Errorf("%w: no confirm Secret %s within %s", ErrConfirmRefused, c.Prefix, timeout)
		case <-tick.C:
		}
		body, err := c.API.Get(ctx, path)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("strimzi: reading the confirm Secret: %w", err)
		}
		if derr := c.API.Delete(ctx, path); derr != nil && !errors.Is(derr, ErrNotFound) {
			return false, fmt.Errorf("strimzi: deleting the confirm Secret: %w", derr)
		}
		return judgeConfirm(body, w.Fingerprint, logged)
	}
}

func judgeConfirm(body []byte, want string, logged time.Time) (bool, error) {
	var s struct {
		Metadata struct {
			CreationTimestamp time.Time `json:"creationTimestamp"`
		} `json:"metadata"`
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &s); err != nil {
		return false, fmt.Errorf("%w: the confirm Secret is not a Secret", ErrConfirmRefused)
	}
	if s.Metadata.CreationTimestamp.Before(logged.Add(-clockSkew)) {
		return false, fmt.Errorf("%w: the confirm Secret was created before the Welcome was logged", ErrConfirmRefused)
	}
	got, err := base64.StdEncoding.DecodeString(s.Data["fingerprint"])
	if err != nil || len(got) == 0 {
		return false, fmt.Errorf("%w: the confirm Secret has no fingerprint key", ErrConfirmRefused)
	}
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(string(got))), []byte(want)) != 1 {
		return false, fmt.Errorf("%w: the fingerprint does not match the Welcome's", ErrConfirmRefused)
	}
	return true, nil
}
