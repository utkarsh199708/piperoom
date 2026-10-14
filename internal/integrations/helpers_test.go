package integrations

import (
	"net"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// canary is a dummy secret; no test output may ever contain it.
const canary = "CANARY-s3cr3t-value-9f2c"

// listenTCP starts a listener that accepts and closes connections, for the
// "reach" checks. It returns the host and port.
func listenTCP(t *testing.T) (string, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	h, p, _ := net.SplitHostPort(ln.Addr().String())
	return h, p
}

// closedPort returns a loopback port nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, p, _ := net.SplitHostPort(ln.Addr().String())
	ln.Close()
	return p
}

func fastDeps() Deps {
	return Deps{CheckTimeout: 300 * time.Millisecond, TestTimeout: 3 * time.Second}
}

func vals(cfg map[string]string, secrets map[string]string) spec.Values {
	v := spec.Values{Config: cfg, Secrets: map[string]netsec.Secret{}}
	for k, s := range secrets {
		v.Secrets[k] = netsec.Secret(s)
	}
	return v
}

// checkByID finds a check or fails the test.
func checkByID(t *testing.T, checks []spec.Check, id string) spec.Check {
	t.Helper()
	for _, c := range checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no check %q in %+v", id, checks)
	return spec.Check{}
}

func wantStatus(t *testing.T, checks []spec.Check, id string, st spec.CheckStatus) spec.Check {
	t.Helper()
	c := checkByID(t, checks, id)
	if c.Status != st {
		t.Fatalf("check %s = %s (%s / %s), want %s", id, c.Status, c.Detail, c.Fix, st)
	}
	return c
}

// noCanary fails if any check text contains the canary.
func noCanary(t *testing.T, checks []spec.Check) {
	t.Helper()
	for _, c := range checks {
		if strings.Contains(c.Title+c.Detail+c.Fix, canary) {
			t.Fatalf("check %s leaks the secret: %+v", c.ID, c)
		}
	}
}
