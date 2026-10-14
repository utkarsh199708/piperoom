package actions

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/schemareg"
)

// validateCases lists every action whose Validate does I/O, with params that
// pass the offline checks, so the first failure can only come from the network.
// hangPath is the one request the server never answers ("" hangs every
// request): a later read in the same Validate must share the deadline too.
var validateCases = []struct {
	action   string
	params   string
	hangPath string
}{
	{"connect_restart_task", `{"connector":"pg-orders-connector","task":0}`, ""},
	{"connect_restart_connector", `{"connector":"pg-orders-connector"}`, ""},
	{"connect_resume_connector", `{"connector":"pg-orders-connector"}`, ""},
	{"connect_resume_connector", `{"connector":"pg-orders-connector"}`, "/connectors/pg-orders-connector/status"},
	{"connect_update_connector_config", updateIn, ""},
	{"connect_update_connector_config", `{"connector":"pg-orders-connector","key":"errors.log.enable","value":"true"}`, "/connectors/pg-orders-connector/config"},
	{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"FULL"}`, ""},
	{"schema_register", `{"subject":"pg.public.orders-value","schema":"{\"type\":\"string\"}"}`, ""},
}

// A target that accepts the request and never answers must not hold Validate
// past its deadline: the room service calls Validate while creating a proposal
// and again right before Execute, and neither passes a context.
func TestValidateReturnsDeadlineErrorWhenTargetNeverAnswers(t *testing.T) {
	for _, tc := range validateCases {
		t.Run(tc.action+tc.hangPath, func(t *testing.T) {
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.hangPath == "" || r.URL.Path == tc.hangPath {
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				switch r.URL.Path {
				case "/connectors":
					w.Write([]byte(`["pg-orders-connector"]`))
				case "/connectors/pg-orders-connector/status":
					w.Write([]byte(`{"name":"pg-orders-connector","connector":{"state":"PAUSED"},"tasks":[]}`))
				case "/subjects":
					w.Write([]byte(`["pg.public.orders-value"]`))
				default:
					http.NotFound(w, r)
				}
			}))
			// Cleanups run last in, first out: free the handlers before Close waits for them.
			t.Cleanup(srv.Close)
			t.Cleanup(func() { close(release) })

			d := Deps{
				Connect:         connect.New(srv.URL, srv.Client()),
				Registry:        schemareg.New(srv.URL, srv.Client()),
				validateTimeout: 50 * time.Millisecond,
			}
			a := mustGet(t, resumeRegistry(t, d), tc.action)

			start := time.Now()
			err := a.Validate(json.RawMessage(tc.params))
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Validate = %v, want a wrapped context.DeadlineExceeded", err)
			}
			// Well under the 10 s default: proves the injected deadline is the one in force.
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("Validate took %v with a 50ms deadline", elapsed)
			}
		})
	}
}

// pausedConnect reports every connector as PAUSED, so resume validates.
type pausedConnect struct{ fakeConnect }

func (pausedConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: "pg-orders-connector", State: "PAUSED"}, nil
}

// The injected deadline must not change the outcome for a target that answers.
func TestValidateNormalPathWithInjectedTimeout(t *testing.T) {
	d := Deps{
		Connect:         &pausedConnect{fakeConnect{names: []string{"pg-orders-connector"}}},
		Registry:        &fakeRegistry{subjects: []string{"pg.public.orders-value"}},
		validateTimeout: 2 * time.Second,
	}
	reg := resumeRegistry(t, d)
	for _, tc := range validateCases {
		if tc.hangPath != "" {
			continue
		}
		t.Run(tc.action, func(t *testing.T) {
			if err := mustGet(t, reg, tc.action).Validate(json.RawMessage(tc.params)); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

// Without an override, Validate uses the 10 s tool default (AGENTS.md rule 4).
func TestValidateContextDefaultsToToolTimeout(t *testing.T) {
	ctx, cancel := Deps{}.validateContext()
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("validate context has no deadline")
	}
	if left := time.Until(dl); left <= 0 || left > timeout {
		t.Fatalf("deadline in %v, want within (0, %v]", left, timeout)
	}
}
