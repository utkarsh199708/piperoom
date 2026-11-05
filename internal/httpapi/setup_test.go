package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Setup rooms over HTTP (docs/api.md "Setup rooms"; U wave, U1-5b): two
// tenants, a driver (alice, member and admin of tenant a), a member of
// tenant a who does not drive (ann), a member of tenant b (bob) and a user
// outside every tenant (mallory).

const setupAPIUsersYAML = `users:
  - {id: alice, name: Alice}
  - {id: ann, name: Ann}
  - {id: bob, name: Bob}
  - {id: mallory, name: Mallory}
`

func setupAPIOwnersYAML() string {
	owner := func(user string) string {
		var b strings.Builder
		b.WriteString("\n    owners:")
		for _, l := range events.LayersOf(events.PipelineKindCDC) {
			b.WriteString("\n      " + l + ": {approvers: [user:" + user + "]}")
		}
		return b.String()
	}
	return "tenants:\n  a: {members: [user:alice, user:ann], admins: [user:alice]}\n  b: {members: [user:bob]}\n" +
		"pipelines:" +
		"\n  - id: existing\n    tenant: a\n    topic_prefix: ex\n    source_tables: [public.e]\n    connectors: [ex-c]" +
		"\n    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}" + owner("alice") + "\n"
}

type setupAPIResolver map[string][]netip.Addr

func (r setupAPIResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if addrs, ok := r[host]; ok {
		return addrs, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

type setupAPIEnv struct {
	srv        *httptest.Server
	api        *httpapi.Server
	svc        *room.Service
	logs       *syncLogBuf
	secretsDir string
}

func newSetupAPIEnv(t *testing.T, enabled bool) *setupAPIEnv {
	t.Helper()
	users, err := room.ParseUsers([]byte(setupAPIUsersYAML))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(setupAPIOwnersYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	secretsDir := filepath.Join(t.TempDir(), "secrets")
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Setup: config.Setup{Enabled: enabled, TokenBudget: 2_000_000, SecretsDir: secretsDir},
		SetupResolver: setupAPIResolver{
			"db.acme":           {netip.MustParseAddr("10.1.2.3")},
			"internal.metadata": {netip.MustParseAddr("169.254.169.254")},
		},
	})
	logs := &syncLogBuf{}
	opts := []httpapi.Option{httpapi.WithSetup(enabled)}
	api := httpapi.New(svc, st, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), opts...)
	srv := httptest.NewServer(api)
	t.Cleanup(func() {
		srv.Close()
		api.Close()
	})
	return &setupAPIEnv{srv: srv, api: api, svc: svc, logs: logs, secretsDir: secretsDir}
}

func (e *setupAPIEnv) do(t *testing.T, user, method, path, body string) (int, []byte) {
	t.Helper()
	resp := doJSON(t, method, e.srv.URL+path, user, body)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (e *setupAPIEnv) errCode(t *testing.T, user, method, path, body string) (int, string) {
	t.Helper()
	resp := doJSON(t, method, e.srv.URL+path, user, body)
	code, _ := errCode(t, resp)
	return resp.StatusCode, code
}

const setupAPIBody = `{"pipeline_id":"orders-setup","tenant":"a","title":"Orders setup",
 "source_kind":"postgres","target":"compose","topic_prefix":"orders",
 "tables":["public.orders"],"connectors":["orders-src"],
 "targets":[{"kind":"postgres","address":"db.acme:5432"}]}`

func TestSetupCreateOverHTTP(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	resp := doJSON(t, http.MethodPost, env.srv.URL+"/api/setup", "alice", setupAPIBody)
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d; body %s", resp.StatusCode, b)
	}
	var out struct {
		Room      room.RoomSnapshot `json:"room"`
		Code      string            `json:"code"`
		ExpiresAt time.Time         `json:"expires_at"`
		Command   string            `json:"command"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if len(out.Code) != 8 || !strings.Contains(out.Command, "--code "+out.Code) {
		t.Fatalf("code/command = %q / %q", out.Code, out.Command)
	}
	if !strings.HasPrefix(out.Command, "piperoom-setup --server ") || !strings.Contains(out.Command, "--server "+env.srv.URL+" ") {
		t.Fatalf("command = %q, want the server origin", out.Command)
	}
	if !out.Room.IsSetupRoom() || out.Room.Setup.Mode != true || out.Room.PipelineID != "orders-setup" {
		t.Fatalf("room = %+v", out.Room)
	}
	if out.Room.Driver == nil || out.Room.Driver.ID != "alice" {
		t.Fatalf("driver = %+v", out.Room.Driver)
	}
	if len(out.Room.Setup.Targets) != 1 || out.Room.Setup.Targets[0].Class != "shared" {
		t.Fatalf("targets = %+v", out.Room.Setup.Targets)
	}
	if out.ExpiresAt.IsZero() {
		t.Fatal("expires_at is zero")
	}
}

func TestSetupCreateRefusals(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	cases := []struct {
		name   string
		user   string
		body   string
		status int
		code   string
	}{
		{"another tenant's member", "bob", setupAPIBody, http.StatusForbidden, "not_tenant_member"},
		{"outside every tenant", "mallory", setupAPIBody, http.StatusForbidden, "not_tenant_member"},
		{"unknown pipeline", "alice", strings.Replace(setupAPIBody, `"orders-setup"`, `"existing"`, 1), http.StatusConflict, "pipeline_exists"},
		{"metadata address", "alice", strings.Replace(setupAPIBody, `"db.acme:5432"`, `"169.254.169.254"`, 1), http.StatusBadRequest, "address_refused"},
		{"metadata by name", "alice", strings.Replace(setupAPIBody, `"db.acme:5432"`, `"metadata.google.internal"`, 1), http.StatusBadRequest, "address_refused"},
		{"metadata by resolution", "alice", strings.Replace(setupAPIBody, `"db.acme:5432"`, `"internal.metadata:5432"`, 1), http.StatusBadRequest, "address_refused"},
		{"bad source kind", "alice", strings.Replace(setupAPIBody, `"postgres"`, `"db2"`, 1), http.StatusBadRequest, "validation"},
		{"unauthenticated", "", setupAPIBody, http.StatusUnauthorized, "session_expired"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, code := env.errCode(t, c.user, http.MethodPost, "/api/setup", c.body)
			if status != c.status || code != c.code {
				t.Fatalf("= %d %s, want %d %s", status, code, c.status, c.code)
			}
		})
	}
}

func TestSetupRoutesDisabled(t *testing.T) {
	env := newSetupAPIEnv(t, false)
	status, code := env.errCode(t, "alice", http.MethodPost, "/api/setup", setupAPIBody)
	if status != http.StatusNotFound || code != "setup_disabled" {
		t.Fatalf("/api/setup = %d %s, want 404 setup_disabled", status, code)
	}
	// A room route names a room that exists, so the setup check answers (an
	// unknown room would be 404 not_found first).
	status, body := env.do(t, "alice", http.MethodPost, "/api/rooms", `{"title":"incident","severity":"sev2","pipeline_id":"existing"}`)
	if status != http.StatusCreated {
		t.Fatalf("create room = %d %s", status, body)
	}
	var inc room.RoomSnapshot
	if err := json.Unmarshal(body, &inc); err != nil {
		t.Fatal(err)
	}
	// The address is one the denylist would refuse: the disabled answer comes
	// first, so a disabled route says nothing about any address.
	status, code = env.errCode(t, "alice", http.MethodPost, "/api/rooms/"+inc.ID+"/setup/targets", `{"kind":"connect","address":"169.254.169.254:8083"}`)
	if status != http.StatusNotFound || code != "setup_disabled" {
		t.Fatalf("targets route = %d %s, want 404 setup_disabled", status, code)
	}
}

func TestSetupRoomRoutes(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	status, body := env.do(t, "alice", http.MethodPost, "/api/setup", setupAPIBody)
	if status != http.StatusCreated {
		t.Fatalf("create = %d %s", status, body)
	}
	var created struct {
		Room room.RoomSnapshot `json:"room"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	roomID := created.Room.ID
	base := "/api/rooms/" + roomID + "/setup"

	// Another tenant's room is a 404, whatever the route.
	status, code := env.errCode(t, "bob", http.MethodPost, base+"/targets", `{"kind":"connect","address":"connect.acme:8083"}`)
	if status != http.StatusNotFound || code != "not_found" {
		t.Fatalf("another tenant = %d %s", status, code)
	}
	// A member who is not the driver is 403 not_driver.
	status, code = env.errCode(t, "ann", http.MethodPost, base+"/targets", `{"kind":"connect","address":"connect.acme:8083"}`)
	if status != http.StatusForbidden || code != "not_driver" {
		t.Fatalf("non-driver = %d %s", status, code)
	}
	// The denylist applies on the route too.
	status, code = env.errCode(t, "alice", http.MethodPost, base+"/targets", `{"kind":"host","address":"[fe80::1]:22"}`)
	if status != http.StatusBadRequest || code != "address_refused" {
		t.Fatalf("link-local = %d %s", status, code)
	}
	// Add a target: 200 and the event.
	status, body = env.do(t, "alice", http.MethodPost, base+"/targets", `{"kind":"connect","address":"connect.acme:8083"}`)
	if status != http.StatusOK {
		t.Fatalf("add target = %d %s", status, body)
	}
	var ev events.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != events.SetupTargetRegistered {
		t.Fatalf("event = %s", ev.Type)
	}
	// Raise a class; a lower one is refused.
	status, body = env.do(t, "alice", http.MethodPost, base+"/targets/"+urlQueryEscape("db.acme:5432")+"/class", `{"class":"production"}`)
	if status != http.StatusOK {
		t.Fatalf("raise = %d %s", status, body)
	}
	status, code = env.errCode(t, "alice", http.MethodPost, base+"/targets/"+urlQueryEscape("db.acme:5432")+"/class", `{"class":"shared"}`)
	if status != http.StatusBadRequest || code != "class_lowered" {
		t.Fatalf("lower = %d %s", status, code)
	}
	status, code = env.errCode(t, "alice", http.MethodPost, base+"/targets/"+urlQueryEscape("no.such:1")+"/class", `{"class":"production"}`)
	if status != http.StatusNotFound || code != "not_found" {
		t.Fatalf("unknown target = %d %s", status, code)
	}
	// Extend the provisional pipeline.
	status, body = env.do(t, "alice", http.MethodPost, base+"/pipeline", `{"add_tables":["public.items"],"add_consumer_groups":["orders-loader"]}`)
	if status != http.StatusOK {
		t.Fatalf("extend = %d %s", status, body)
	}
	if err := json.Unmarshal(body, &ev); err != nil || ev.Type != events.SetupPipelineExtended {
		t.Fatalf("extend event = %s err %v", ev.Type, err)
	}
	status, code = env.errCode(t, "alice", http.MethodPost, base+"/pipeline", `{"add_tables":["public.orders"]}`)
	if status != http.StatusBadRequest || code != "validation" {
		t.Fatalf("duplicate table = %d %s", status, code)
	}
	// The maintenance window.
	now := time.Now().UTC()
	window := `{"start":"` + now.Add(10*time.Minute).Format(time.RFC3339) + `","end":"` + now.Add(70*time.Minute).Format(time.RFC3339) + `"}`
	status, body = env.do(t, "alice", http.MethodPost, base+"/maintenance-window", window)
	if status != http.StatusOK {
		t.Fatalf("window = %d %s", status, body)
	}
	long := `{"start":"` + now.Add(10*time.Minute).Format(time.RFC3339) + `","end":"` + now.Add(5*time.Hour).Format(time.RFC3339) + `"}`
	status, code = env.errCode(t, "alice", http.MethodPost, base+"/maintenance-window", long)
	if status != http.StatusBadRequest || code != "window_too_long" {
		t.Fatalf("long window = %d %s", status, code)
	}
	status, code = env.errCode(t, "alice", http.MethodPost, base+"/maintenance-window", `{"start":"soon","end":"later"}`)
	if status != http.StatusBadRequest || code != "validation" {
		t.Fatalf("bad times = %d %s", status, code)
	}

	// The event sequence, as the room's log holds it.
	resp := doJSON(t, http.MethodGet, env.srv.URL+"/api/rooms/"+roomID+"/events?limit=100", "alice", "")
	defer resp.Body.Close()
	var page struct {
		Events []events.Event `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	var got []events.Type
	for _, e := range page.Events {
		got = append(got, e.Type)
	}
	want := []events.Type{
		events.RoomCreated, events.SetupTargetClassed, events.ControlGranted,
		events.SetupTargetRegistered, events.SetupTargetClassed,
		events.SetupTargetClassed,
		events.SetupPipelineExtended,
		events.SetupMaintenanceWindow,
	}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}

	// The room view folded it all.
	resp = doJSON(t, http.MethodGet, env.srv.URL+"/api/rooms/"+roomID, "alice", "")
	defer resp.Body.Close()
	var snap room.RoomSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Setup == nil || len(snap.Setup.Targets) != 2 {
		t.Fatalf("setup = %+v", snap.Setup)
	}
	if snap.Setup.Targets[1].Address != "connect.acme:8083" || snap.Setup.Targets[1].Class != "shared" {
		t.Fatalf("targets = %+v", snap.Setup.Targets)
	}
	if snap.Setup.MaintenanceWindow == nil {
		t.Fatal("maintenance window missing")
	}
	if len(snap.Setup.Provisional.Tables) != 2 {
		t.Fatalf("provisional = %+v", snap.Setup.Provisional)
	}
}

// urlQueryEscape escapes a target address for the class route's path segment.
func urlQueryEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "/", "%2F"), ":", "%3A")
}

func TestSetupBodyCap(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	big := `{"pipeline_id":"orders-setup","tenant":"a","title":"` + strings.Repeat("x", 70<<10) + `"}`
	status, code := env.errCode(t, "alice", http.MethodPost, "/api/setup", big)
	if status != http.StatusRequestEntityTooLarge || code != "too_large" {
		t.Fatalf("= %d %s, want 413 too_large", status, code)
	}
}

// TestSetupRoomInList: GET /api/rooms carries mode, runner.connected and phase
// only.
func TestSetupRoomInList(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	if status, body := env.do(t, "alice", http.MethodPost, "/api/setup", setupAPIBody); status != http.StatusCreated {
		t.Fatalf("create = %d %s", status, body)
	}
	resp := doJSON(t, http.MethodGet, env.srv.URL+"/api/rooms", "alice", "")
	defer resp.Body.Close()
	var rows []struct {
		ID    string         `json:"id"`
		Setup map[string]any `json:"setup"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Setup == nil {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].Setup["mode"] != true {
		t.Fatalf("mode = %v", rows[0].Setup["mode"])
	}
	for _, key := range []string{"targets", "provisional", "plan_id", "bounds"} {
		if _, ok := rows[0].Setup[key]; ok {
			t.Fatalf("list row carries setup.%s: %+v", key, rows[0].Setup)
		}
	}
}
