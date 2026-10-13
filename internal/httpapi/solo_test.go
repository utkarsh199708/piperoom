package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Solo mode over HTTP (docs/design/auth.md section 5.3): the solo user may
// approve every layer, but each approval must carry confirm
// "<pipeline_id>/<layer>", and the grant is "solo".
func TestSoloModeApprovalNeedsConfirm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.yaml")
	src := "pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\nlayers:\n  postgres: priya\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := actions.NewRegistry()
	for _, l := range []string{"postgres", "kafka"} {
		if err := reg.Register(&testAction{layer: l}); err != nil {
			t.Fatal(err)
		}
	}
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: own, Actions: reg, Logger: log, SoloUser: "priya",
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	api := httpapi.New(svc, st, log, httpapi.WithSessions(auth.NewMemStore()))
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close() })

	priya, _ := svc.LookupUser("priya")
	snap, err := svc.CreateRoom(context.Background(), priya, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	newProposal := func(layer string) string {
		pid, err := svc.CreateProposal(context.Background(), snap.ID, events.ProposalCreatedPayload{
			Title: "fix", Risk: "low risk", Rollback: "undo it",
			Steps: []events.ProposalStep{{Action: "fix_" + layer, Layer: layer, Params: json.RawMessage(`{}`), Description: "d"}},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	hashOf := func(pid string) string {
		rooms, err := svc.ListRooms(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rooms {
			for _, p := range r.OpenProposals {
				if p.ProposalID == pid {
					return p.ParamsHash
				}
			}
		}
		t.Fatalf("proposal %s is not open", pid)
		return ""
	}
	// approve sends the body with the proposal's params_hash added, as the web app does.
	approve := func(user, pid, body string) *http.Response {
		body = strings.Replace(body, "{", `{"params_hash":"`+hashOf(pid)+`",`, 1)
		return doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/approve", user, body)
	}

	pid := newProposal("kafka") // a layer priya does not own in owners.yaml: only solo gives it
	for name, body := range map[string]string{
		"no confirm":      `{"layer":"kafka"}`,
		"wrong layer":     `{"layer":"kafka","confirm":"orders/postgres"}`,
		"wrong pipeline":  `{"layer":"kafka","confirm":"other/kafka"}`,
		"wrong case":      `{"layer":"kafka","confirm":"Orders/Kafka"}`,
		"surrounding gap": `{"layer":"kafka","confirm":" orders/kafka"}`,
	} {
		resp := approve("priya", pid, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, resp.StatusCode)
		}
		resp.Body.Close()
	}
	// The normal rule is unchanged for everybody else, and their confirm is ignored.
	resp := approve("arjun", pid, `{"layer":"kafka","confirm":"orders/kafka"}`)
	if resp.StatusCode != http.StatusOK { // arjun owns kafka in the default users.yaml
		t.Fatalf("arjun (the users.yaml owner) = %d", resp.StatusCode)
	}
	resp.Body.Close()

	pid = newProposal("kafka")
	resp = approve("priya", pid, fmt.Sprintf(`{"layer":%q,"confirm":%q}`, "kafka", "orders/kafka"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("solo approval with the right confirm = %d", resp.StatusCode)
	}
	resp.Body.Close()
	if !strings.Contains(logs.String(), "grant=solo") {
		t.Fatalf("the grant is not 'solo' in the log:\n%s", logs.String())
	}

	// /api/me lists every layer for the solo user.
	resp = doJSON(t, http.MethodGet, srv.URL+"/api/me", "priya", "")
	var me struct {
		Layers []string `json:"layers"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&me)
	resp.Body.Close()
	if len(me.Layers) != 6 {
		t.Fatalf("solo layers = %v, want all six", me.Layers)
	}
}
