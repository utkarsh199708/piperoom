package integrations

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/integrations/spec"
)

// Check deadlines (integrations.md §4.2).
const (
	DefaultCheckTimeout = 5 * time.Second
	DefaultTestTimeout  = 20 * time.Second
	maxDetail           = 300
)

// outcome is what one check function reports.
type outcome struct {
	status spec.CheckStatus
	detail string
	fix    string
}

func pass(detail string) outcome { return outcome{status: spec.CheckPass, detail: detail} }
func fail(detail, fix string) outcome {
	return outcome{status: spec.CheckFail, detail: detail, fix: fix}
}
func warn(detail, fix string) outcome {
	return outcome{status: spec.CheckWarn, detail: detail, fix: fix}
}
func skippedOutcome(detail string) outcome { return outcome{status: spec.CheckSkipped, detail: detail} }

// checklist runs checks one by one, each with its own deadline inside the
// whole test's deadline, and scrubs every detail of the secrets it was given.
type checklist struct {
	ctx     context.Context
	per     time.Duration
	secrets []string
	checks  []spec.Check
}

// newChecklist starts a checklist. v's secrets are scrubbed from every
// detail and fix, in case a driver error echoes one.
func newChecklist(ctx context.Context, per time.Duration, v spec.Values) *checklist {
	if per <= 0 {
		per = DefaultCheckTimeout
	}
	cl := &checklist{ctx: ctx, per: per}
	for _, s := range v.Secrets {
		if r := s.Reveal(); len(r) >= 4 {
			cl.secrets = append(cl.secrets, r)
		}
	}
	return cl
}

// run runs fn with a per-check deadline and records its outcome.
func (cl *checklist) run(id, title string, fn func(ctx context.Context) outcome) spec.CheckStatus {
	if err := cl.ctx.Err(); err != nil {
		cl.add(id, title, fail("the whole test ran out of time ("+DefaultTestTimeout.String()+")", ""))
		return spec.CheckFail
	}
	ctx, cancel := context.WithTimeout(cl.ctx, cl.per)
	defer cancel()
	o := fn(ctx)
	if o.status == spec.CheckFail && errors.Is(ctx.Err(), context.DeadlineExceeded) && !strings.Contains(o.detail, "timed out") {
		o.detail = strings.TrimSpace("timed out after " + cl.per.String() + ". " + o.detail)
	}
	cl.add(id, title, o)
	return o.status
}

// skip records a skipped check.
func (cl *checklist) skip(id, title, why string) { cl.add(id, title, skippedOutcome(why)) }

func (cl *checklist) add(id, title string, o outcome) {
	cl.checks = append(cl.checks, spec.Check{
		ID: id, Title: title, Status: o.status,
		Detail: cl.scrub(o.detail), Fix: cl.scrub(o.fix),
	})
}

func (cl *checklist) scrub(s string) string {
	for _, sec := range cl.secrets {
		s = strings.ReplaceAll(s, sec, "[redacted]")
	}
	return truncate(s, maxDetail)
}

// result returns the checks; never nil.
func (cl *checklist) result() []spec.Check {
	if cl.checks == nil {
		return []spec.Check{}
	}
	return cl.checks
}

// errText is a one-line description of err for a check detail. It does not
// truncate: the checklist scrubs secrets from the full text first and only
// then cuts it, so a secret straddling the cut cannot leak its prefix.
func errText(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timed out"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timed out"
	}
	return strings.Join(strings.Fields(err.Error()), " ")
}

// truncate cuts s to at most n bytes on a rune boundary.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n - len("…")
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// testCtx bounds a whole test.
func testCtx(ctx context.Context, total time.Duration) (context.Context, context.CancelFunc) {
	if total <= 0 {
		total = DefaultTestTimeout
	}
	return context.WithTimeout(ctx, total)
}

// reachCheck dials host:port through the guard and closes the connection.
func reachCheck(dial dialFunc, host, port string) func(ctx context.Context) outcome {
	return func(ctx context.Context) outcome {
		addr := net.JoinHostPort(host, port)
		c, err := dial(ctx, "tcp", addr)
		if err != nil {
			if errors.Is(err, ErrBlockedHost) {
				return fail(errText(err), "Use a host allowed by PIPEROOM_INTEGRATION_ALLOWED_HOSTS, or ask the Piperoom operator to add it.")
			}
			return fail(fmt.Sprintf("cannot connect to %s: %s", addr, errText(err)),
				fmt.Sprintf("Allow TCP from the Piperoom server to %s (firewall rule or security group), and check the host and port.", addr))
		}
		_ = c.Close()
		return pass("")
	}
}

// dialFunc has the signature of net.Dialer.DialContext.
type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)
