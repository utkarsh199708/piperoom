package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// kindServer serves an owners.yaml written verbatim, dev X-User mode, with
// meera (warehouse in the default users.yaml) as the optional solo user.
func kindServer(t *testing.T, ownersYAML, solo string) *httptest.Server {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(ownersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: own, SoloUser: solo,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		}})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close(); svc.Close() })
	return srv
}

const (
	kindOrders  = "  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n"
	kindRevenue = "  - id: revenue\n    title: Revenue\n    kind: batch\n    dags: [revenue_daily, shared_dag]\n    models: [fct_revenue]\n"
	kindBilling = "  - id: billing\n    kind: batch\n    dags: [billing_daily, shared_dag]\n"
)

func getJSON(t *testing.T, url, user string, out any) {
	t.Helper()
	resp := doJSON(t, http.MethodGet, url, user, "")
	defer resp.Body.Close()
	wantStatus(t, resp, http.StatusOK)
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func TestPipelinesListKind(t *testing.T) {
	srv := kindServer(t, "pipelines:\n"+kindOrders+kindRevenue, "")
	var raw []map[string]any
	getJSON(t, srv.URL+"/api/pipelines", "arjun", &raw)
	want := []map[string]any{
		{"id": "orders", "title": "", "shadow": false, "kind": "cdc"}, // no kind: → cdc
		{"id": "revenue", "title": "Revenue", "shadow": false, "kind": "batch"},
	}
	if !reflect.DeepEqual(raw, want) {
		t.Fatalf("pipelines = %v, want %v", raw, want)
	}
}

func TestMeLayersScopedByKind(t *testing.T) {
	me := func(t *testing.T, srv *httptest.Server, user string) []string {
		t.Helper()
		var out struct {
			Layers []string `json:"layers"`
		}
		getJSON(t, srv.URL+"/api/me", user, &out)
		return out.Layers
	}
	t.Run("cdc only: no orchestrator or transform", func(t *testing.T) {
		srv := kindServer(t, "pipelines:\n"+kindOrders, "meera")
		if got, want := me(t, srv, "meera"), events.LayersOf(events.PipelineKindCDC); !slices.Equal(got, want) {
			t.Fatalf("solo = %v, want %v", got, want)
		}
	})
	t.Run("batch present: they appear", func(t *testing.T) {
		srv := kindServer(t, "pipelines:\n"+kindOrders+kindRevenue, "meera")
		// Every layer of the kinds in use: no pipeline has source_kind mysql
		// (Q8-0), so the mysql layer is not one of them.
		want := slices.DeleteFunc(slices.Clone(events.Layers), func(l string) bool { return l == events.LayerMySQL })
		if got := me(t, srv, "meera"); !slices.Equal(got, want) {
			t.Fatalf("solo = %v, want %v", got, want)
		}
		// A non-solo user keeps their own layers.
		if got := me(t, srv, "arjun"); !slices.Equal(got, []string{"kafka-connect", "kafka"}) {
			t.Fatalf("arjun = %v", got)
		}
	})
	t.Run("batch only: users.yaml CDC layers drop out", func(t *testing.T) {
		srv := kindServer(t, "pipelines:\n"+kindRevenue, "")
		if got := me(t, srv, "arjun"); len(got) != 0 {
			t.Fatalf("arjun = %v, want []", got)
		}
		if got := me(t, srv, "meera"); !slices.Equal(got, []string{"warehouse"}) {
			t.Fatalf("meera = %v, want [warehouse]", got)
		}
	})
}

func TestAlertWebhookDAGID(t *testing.T) {
	srv := kindServer(t, "pipelines:\n"+kindOrders+kindRevenue+kindBilling, "")
	for _, tc := range []struct {
		name, body string
		status     int
		want       string
	}{
		{"dag_id picks the batch pipeline", `{"title":"a","dag_id":"revenue_daily"}`, 201, "revenue"},
		{"dag_id of the other pipeline", `{"title":"b","dag_id":"billing_daily"}`, 201, "billing"},
		{"explicit pipeline_id wins", `{"title":"c","pipeline_id":"orders","dag_id":"revenue_daily"}`, 201, "orders"},
		// Without a usable pipeline_id or dag_id the body resolves exactly as
		// before dag_id was read: the room opens on "unknown" (proposals off).
		{"shared dag: unchanged fallback", `{"title":"d","dag_id":"shared_dag"}`, 201, "unknown"},
		{"unknown dag: unchanged fallback", `{"title":"e","dag_id":"nope"}`, 201, "unknown"},
		{"case-sensitive", `{"title":"f","dag_id":"Revenue_Daily"}`, 201, "unknown"},
		{"non-string dag_id is ignored", `{"title":"g","dag_id":7}`, 201, "unknown"},
		{"bad pipeline_id and no dag match: 400 as before", `{"title":"h","pipeline_id":"bad id!","dag_id":"nope"}`, 400, ""},
		{"bad pipeline_id: dag_id resolves", `{"title":"i","pipeline_id":"bad id!","dag_id":"revenue_daily"}`, 201, "revenue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun", tc.body)
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
			if tc.status != http.StatusCreated {
				return
			}
			var snap struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
				t.Fatal(err)
			}
			if snap.PipelineID != tc.want {
				t.Fatalf("pipeline_id = %q, want %q", snap.PipelineID, tc.want)
			}
		})
	}
}
