package integrations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/integrations/spec"
)

// A driver error that echoes a secret near the truncation point must not leak
// the secret's prefix: scrubbing runs on the full text, cutting after.
func TestCheckDetailScrubsBeforeTruncating(t *testing.T) {
	v := vals(nil, map[string]string{"password": canary})
	for _, at := range []int{150, 190, 195, 280, 290, 298} {
		cl := newChecklist(context.Background(), time.Second, v)
		msg := strings.Repeat("x", at) + canary + strings.Repeat("y", 200)
		cl.run("login", "Login", func(context.Context) outcome {
			return fail("login failed: "+errText(errors.New(msg)), "fix "+errText(errors.New(msg)))
		})
		c := wantStatus(t, cl.result(), "login", spec.CheckFail)
		for _, s := range []string{c.Detail, c.Fix} {
			if strings.Contains(s, canary[:6]) {
				t.Fatalf("at %d: secret prefix leaked: %q", at, s)
			}
			if len(s) > maxDetail {
				t.Fatalf("at %d: %d bytes, over %d", at, len(s), maxDetail)
			}
		}
	}
}
