package room_test

import (
	"context"
	"encoding/json"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Setup rooms (U wave; docs/design/setup-agent.md, docs/api.md "Setup rooms").
// Two tenants: tenant a holds alice (member and admin) and ann (member);
// tenant b holds bob (member) and ben (member). mallory is in no tenant.

const setupUsers = `users:
  - {id: alice, name: Alice}
  - {id: ann, name: Ann}
  - {id: bob, name: Bob}
  - {id: ben, name: Ben}
  - {id: mallory, name: Mallory}
`

func setupOwnersYAML(withTenants bool) string {
	owner := func(user string) string {
		var b strings.Builder
		b.WriteString("\n    owners:")
		for _, l := range events.LayersOf(events.PipelineKindCDC) {
			b.WriteString("\n      " + l + ": {approvers: [user:" + user + "]}")
		}
		return b.String()
	}
	tenant := func(t string) string {
		if !withTenants {
			return ""
		}
		return "\n    tenant: " + t
	}
	y := "pipelines:" +
		"\n  - id: existing" + tenant("a") + "\n    topic_prefix: ex\n    source_tables: [public.e]\n    connectors: [ex-c]" +
		"\n    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}" + owner("alice") +
		"\n  - id: other" + tenant("b") + "\n    topic_prefix: ot\n    source_tables: [public.o]\n    connectors: [ot-c]" +
		"\n    connections: {source_db_url: {env: OT_SRC}, warehouse_db_url: {env: OT_WH}, kafka_brokers: {env: OT_K}, connect_url: {env: OT_C}, schema_registry_url: {env: OT_SR}}" + owner("bob") + "\n"
	if withTenants {
		y = "tenants:\n  a: {members: [user:alice, user:ann], admins: [user:alice]}\n  b: {members: [user:bob, user:ben]}\n" + y
	}
	return y
}

// fakeResolver answers the names a test lists and reports every other name as
// not found, which the server accepts (the runner checks it at dial time).
type fakeResolver map[string][]netip.Addr

func (r fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if addrs, ok := r[host]; ok {
		return addrs, nil
	}
	return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
}

type setupEnv struct {
	svc   *room.Service
	store store.Store
	own   *owners.Owners
	now   time.Time
}

type setupOptions struct {
	disabled    bool
	local       bool
	ownAddrs    []netip.Addr
	prod        []string
	budget      int64
	resolver    room.TargetResolver
	withTenants bool
}

func newSetupEnv(t *testing.T, o setupOptions) *setupEnv {
	t.Helper()
	users, err := room.ParseUsers([]byte(setupUsers))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(setupOwnersYAML(o.withTenants)), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	res := o.resolver
	if res == nil {
		res = fakeResolver{"db.acme": {netip.MustParseAddr("10.1.2.3")}} // private is allowed
	}
	budget := o.budget
	if budget == 0 {
		budget = 2_000_000
	}
	st := memstore.New()
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	svc := room.New(room.Deps{
		Store: st, Users: users, Owners: own,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Now:           func() time.Time { return now },
		Setup:         config.Setup{Enabled: !o.disabled, TokenBudget: budget, ProdPatterns: o.prod},
		SetupLocal:    o.local,
		SetupOwnAddrs: o.ownAddrs,
		SetupResolver: res,
	})
	t.Cleanup(svc.Close)
	return &setupEnv{svc: svc, store: st, own: own, now: now}
}

func alice() room.User   { return room.User{ID: "alice", Name: "Alice"} }
func ann() room.User     { return room.User{ID: "ann", Name: "Ann"} }
func bob() room.User     { return room.User{ID: "bob", Name: "Bob"} }
func mallory() room.User { return room.User{ID: "mallory", Name: "Mallory"} }

func setupRequest(id string) events.SetupRequest {
	return events.SetupRequest{
		PipelineID:  id,
		Tenant:      "a",
		Title:       "Orders setup",
		SourceKind:  "postgres",
		Target:      "compose",
		TopicPrefix: "orders",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"orders-src"},
		Targets:     []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	}
}

func createSetup(t *testing.T, env *setupEnv, req events.SetupRequest) room.SetupRoom {
	t.Helper()
	res, err := env.svc.CreateSetupRoom(context.Background(), alice(), req)
	if err != nil {
		t.Fatalf("CreateSetupRoom: %v", err)
	}
	return res
}

// eventTypes returns the room's event types in order.
func eventTypes(t *testing.T, env *setupEnv, roomID string) []events.Type {
	t.Helper()
	evs, _, err := env.svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	out := make([]events.Type, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func wantSetupError(t *testing.T, err error, status int, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s error, got nil", code)
	}
	re, ok := err.(*room.Error)
	if !ok {
		t.Fatalf("want *room.Error, got %T: %v", err, err)
	}
	if re.Status != status || re.Code != code {
		t.Fatalf("error = %d %s (%q), want %d %s", re.Status, re.Code, re.Msg, status, code)
	}
}

func TestCreateSetupRoomHappyPath(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	ctx := context.Background()
	res := createSetup(t, env, setupRequest("orders-setup"))

	if len(res.Code) != 8 {
		t.Fatalf("pairing code = %q, want 8 characters", res.Code)
	}
	for _, r := range res.Code {
		if !strings.ContainsRune("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", r) {
			t.Fatalf("pairing code %q holds %q, outside the alphabet", res.Code, r)
		}
	}
	if !res.ExpiresAt.After(env.now) || res.ExpiresAt.Sub(env.now) != 10*time.Minute {
		t.Fatalf("expires_at = %v, want now + 10 min", res.ExpiresAt)
	}

	snap := res.Room
	if !snap.IsSetupRoom() || snap.Setup == nil || !snap.Setup.Mode {
		t.Fatalf("room is not a setup room: %+v", snap.Setup)
	}
	if snap.Title != "Orders setup" || snap.PipelineID != "orders-setup" || snap.SetupTenant() != "a" {
		t.Fatalf("snapshot title/pipeline/tenant = %q/%q/%q", snap.Title, snap.PipelineID, snap.SetupTenant())
	}
	if snap.Driver == nil || snap.Driver.ID != "alice" {
		t.Fatalf("creator is not the driver: %+v", snap.Driver)
	}
	if len(snap.Setup.Targets) != 1 || snap.Setup.Targets[0].Address != "db.acme:5432" || snap.Setup.Targets[0].Class != "shared" {
		t.Fatalf("targets = %+v", snap.Setup.Targets)
	}
	p := snap.Setup.Provisional
	if p.TopicPrefix != "orders" || len(p.Tables) != 1 || p.Tables[0] != "public.orders" || len(p.Connectors) != 1 {
		t.Fatalf("provisional = %+v", p)
	}
	if snap.Setup.Bounds.AttemptsPerStep != 3 || snap.Setup.Bounds.StepsPerPlan != 12 || snap.Setup.Bounds.WallClockS != 2700 {
		t.Fatalf("bounds = %+v", snap.Setup.Bounds)
	}
	if snap.Setup.Verified == nil || snap.Setup.Runner != nil {
		t.Fatalf("verified = %v, runner = %v; want [] and nil", snap.Setup.Verified, snap.Setup.Runner)
	}

	// The room's view carries the configured token budget (RoomFor).
	view, err := env.svc.RoomFor(ctx, alice(), snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Setup.Bounds.TokenBudget != 2_000_000 {
		t.Fatalf("token budget = %d", view.Setup.Bounds.TokenBudget)
	}

	// The provisional pipeline the room is scoped to is owners.Provisional's.
	pp, ok := snap.SetupProvisional()
	if !ok || pp.ID != "orders-setup" || pp.TopicPrefix != "orders" || len(pp.SourceTables) != 1 || len(pp.Connectors) != 1 {
		t.Fatalf("provisional pipeline = %+v, ok %v", pp, ok)
	}
	if len(pp.Owners) != 0 {
		t.Fatalf("provisional pipeline carries owners: %+v", pp.Owners)
	}

	// The event sequence: room.created (mode setup, the request as alert),
	// the request's class decision, then the creator's grant.
	want := []events.Type{events.RoomCreated, events.SetupTargetClassed, events.ControlGranted}
	got := eventTypes(t, env, snap.ID)
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
	evs, _, _ := env.svc.Events(ctx, snap.ID, 0, 1000)
	var created events.RoomCreatedPayload
	if err := json.Unmarshal(evs[0].Payload, &created); err != nil {
		t.Fatal(err)
	}
	if created.Mode != events.RoomModeSetup || created.Shadow {
		t.Fatalf("room.created mode/shadow = %q/%v", created.Mode, created.Shadow)
	}
	var req events.SetupRequest
	if err := json.Unmarshal(created.Alert, &req); err != nil {
		t.Fatalf("room.created.alert is not the SetupRequest: %v", err)
	}
	if req.PipelineID != "orders-setup" || req.Tenant != "a" {
		t.Fatalf("room.created.alert = %+v", req)
	}
}

func TestCreateSetupRoomTenancy(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	cases := []struct {
		name   string
		user   room.User
		tenant string
		status int
	}{
		{"another tenant's member", bob(), "a", 403},
		{"unknown tenant", alice(), "no-such-tenant", 403},
		{"no tenant", alice(), "", 400},
		{"no tenant in any, tenancy on", mallory(), "a", 403},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := setupRequest("orders-" + strings.ReplaceAll(c.name, " ", "-"))
			req.Tenant = c.tenant
			_, err := env.svc.CreateSetupRoom(context.Background(), c.user, req)
			code := "not_tenant_member"
			if c.status == 400 {
				code = "validation"
			}
			wantSetupError(t, err, c.status, code)
		})
	}
	rooms, err := env.svc.ListRooms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rooms) != 0 {
		t.Fatalf("a refused request opened a room: %+v", rooms)
	}
}

func TestCreateSetupRoomWithoutTenancy(t *testing.T) {
	env := newSetupEnv(t, setupOptions{})
	// Without a tenants: block the tenant must be absent.
	req := setupRequest("orders-setup")
	if _, err := env.svc.CreateSetupRoom(context.Background(), alice(), req); err == nil {
		t.Fatal("a tenant on a server without tenants: was accepted")
	}
	req.Tenant = ""
	if _, err := env.svc.CreateSetupRoom(context.Background(), alice(), req); err != nil {
		t.Fatalf("CreateSetupRoom: %v", err)
	}
}

func TestCreateSetupRoomDisabled(t *testing.T) {
	env := newSetupEnv(t, setupOptions{disabled: true, withTenants: true})
	_, err := env.svc.CreateSetupRoom(context.Background(), alice(), setupRequest("orders-setup"))
	wantSetupError(t, err, 404, "setup_disabled")
}

func TestCreateSetupRoomPipelineExists(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	req := setupRequest("existing")
	_, err := env.svc.CreateSetupRoom(context.Background(), alice(), req)
	wantSetupError(t, err, 409, "pipeline_exists")

	// The same id again while its setup room is open is the same answer.
	createSetup(t, env, setupRequest("orders-setup"))
	_, err = env.svc.CreateSetupRoom(context.Background(), alice(), setupRequest("orders-setup"))
	wantSetupError(t, err, 409, "pipeline_exists")
}

func TestCreateSetupRoomBadRequest(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	cases := []struct {
		name string
		edit func(*events.SetupRequest)
		code string
	}{
		{"pipeline id shape", func(r *events.SetupRequest) { r.PipelineID = "bad id" }, "validation"},
		{"source kind", func(r *events.SetupRequest) { r.SourceKind = "db2" }, "validation"},
		{"target", func(r *events.SetupRequest) { r.Target = "laptop" }, "validation"},
		{"topic prefix", func(r *events.SetupRequest) { r.TopicPrefix = "" }, "validation"},
		{"no tables", func(r *events.SetupRequest) { r.Tables = nil }, "validation"},
		{"bad table", func(r *events.SetupRequest) { r.Tables = []string{"orders"} }, "validation"},
		{"duplicate table", func(r *events.SetupRequest) { r.Tables = []string{"public.a", "public.a"} }, "validation"},
		{"bad connector", func(r *events.SetupRequest) { r.Connectors = []string{"has space"} }, "validation"},
		{"title", func(r *events.SetupRequest) { r.Title = "" }, "validation"},
		{"duplicate target", func(r *events.SetupRequest) {
			r.Targets = []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}, {Kind: "host", Address: "db.acme:5432"}}
		}, "validation"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := setupRequest("orders-" + strings.ReplaceAll(c.name, " ", "-"))
			c.edit(&req)
			_, err := env.svc.CreateSetupRoom(context.Background(), alice(), req)
			wantSetupError(t, err, 400, c.code)
		})
	}
}

func TestCreateSetupRoomTargetDenylist(t *testing.T) {
	own := []netip.Addr{netip.MustParseAddr("10.9.9.9")}
	res := fakeResolver{
		"db.acme":           {netip.MustParseAddr("10.1.2.3")},        // private: allowed
		"internal.metadata": {netip.MustParseAddr("169.254.169.254")}, // link-local: refused
		"unresolvable.acme": nil,
	}
	env := newSetupEnv(t, setupOptions{withTenants: true, ownAddrs: own, resolver: res})
	refused := []string{
		"169.254.169.254:5432",     // link-local (cloud metadata)
		"fd00:ec2::254",            // the EC2 metadata IPv6 address
		"[fe80::1]:5432",           // link-local IPv6
		"224.0.0.1:9092",           // multicast
		"0.0.0.0:5432",             // unspecified
		"127.0.0.1:5432",           // another host's loopback on a team server
		"metadata.google.internal", // refused by name
		"internal.metadata:5432",   // resolves to a link-local address
		"10.9.9.9:5432",            // this server's own address
	}
	for i, address := range refused {
		req := setupRequest("orders-refused-" + string(rune('a'+i)))
		req.Targets = []events.SetupTarget{{Kind: "postgres", Address: address}}
		_, err := env.svc.CreateSetupRoom(context.Background(), alice(), req)
		wantSetupError(t, err, 400, "address_refused")
	}
	allowed := []string{"db.acme:5432", "unresolvable.acme:5432", "postgres://reader@db.acme:5432/orders"}
	for i, address := range allowed {
		req := setupRequest("orders-allowed-" + string(rune('a'+i)))
		req.Targets = []events.SetupTarget{{Kind: "postgres", Address: address}}
		if _, err := env.svc.CreateSetupRoom(context.Background(), alice(), req); err != nil {
			t.Fatalf("address %q was refused: %v", address, err)
		}
	}
}

func TestCreateSetupRoomLoopbackOnLocalServer(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true, local: true})
	req := setupRequest("orders-loopback")
	req.Targets = []events.SetupTarget{
		{Kind: "postgres", Address: "127.0.0.1:5432"},
		{Kind: "kafka", Address: "[::1]:9092"},
	}
	if _, err := env.svc.CreateSetupRoom(context.Background(), alice(), req); err != nil {
		t.Fatalf("a local server refused a loopback target: %v", err)
	}
}

func TestCreateSetupRoomClasses(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true, prod: []string{"*.prod.acme", "prod-cluster/*"}})
	ctx := context.Background()

	// A production pattern raises the derived class.
	res := createSetup(t, env, events.SetupRequest{
		PipelineID: "orders-prod", Tenant: "a", Title: "Prod", SourceKind: "postgres", Target: "compose",
		TopicPrefix: "prod", Tables: []string{"public.orders"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.prod.acme:5432"}},
	})
	if res.Room.Setup.Targets[0].Class != "production" {
		t.Fatalf("class = %q, want production", res.Room.Setup.Targets[0].Class)
	}

	// A person's floor above the derived class is kept.
	req := setupRequest("orders-raised")
	req.Targets = []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432", Class: "production"}}
	res = createSetup(t, env, req)
	if res.Room.Setup.Targets[0].Class != "production" {
		t.Fatalf("class = %q, want production", res.Room.Setup.Targets[0].Class)
	}

	// A floor below the derived class, or below shared, is refused.
	for _, c := range []struct {
		class string
		addr  string
	}{
		{"local", "db.acme:5432"},
		{"shared", "db.prod.acme:5432"},
		{"local", "db.prod.acme:5432"},
	} {
		req := setupRequest("orders-lowered-" + c.class + strings.ReplaceAll(c.addr, ":", "-"))
		req.Targets = []events.SetupTarget{{Kind: "postgres", Address: c.addr, Class: c.class}}
		_, err := env.svc.CreateSetupRoom(ctx, alice(), req)
		wantSetupError(t, err, 400, "class_lowered")
	}
}

func TestSetupDriverCommands(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	ctx := context.Background()
	res := createSetup(t, env, setupRequest("orders-setup"))
	roomID := res.Room.ID

	// A member who is not the driver cannot click.
	_, err := env.svc.AddSetupTarget(ctx, ann(), roomID, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
	wantSetupError(t, err, 403, "not_driver")
	_, err = env.svc.ExtendSetupPipeline(ctx, ann(), roomID, room.SetupPipelineExtension{AddTables: []string{"public.items"}})
	wantSetupError(t, err, 403, "not_driver")
	_, err = env.svc.RecordMaintenanceWindow(ctx, ann(), roomID, env.now, env.now.Add(time.Hour))
	wantSetupError(t, err, 403, "not_driver")
	_, err = env.svc.RaiseSetupTargetClass(ctx, ann(), roomID, "db.acme:5432", "production")
	wantSetupError(t, err, 403, "not_driver")

	// A member of another tenant cannot see the room at all.
	_, err = env.svc.AddSetupTarget(ctx, bob(), roomID, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
	wantSetupError(t, err, 404, "not_found")

	// Add a target: registered, then classed.
	ev, err := env.svc.AddSetupTarget(ctx, alice(), roomID, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
	if err != nil {
		t.Fatalf("AddSetupTarget: %v", err)
	}
	if ev.Type != events.SetupTargetRegistered {
		t.Fatalf("event = %s", ev.Type)
	}
	_, err = env.svc.AddSetupTarget(ctx, alice(), roomID, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
	wantSetupError(t, err, 400, "validation")

	// A bad address is refused before anything is written.
	_, err = env.svc.AddSetupTarget(ctx, alice(), roomID, events.SetupTarget{Kind: "host", Address: "169.254.169.254"})
	wantSetupError(t, err, 400, "address_refused")

	// Raise a class, never lower it.
	if _, err := env.svc.RaiseSetupTargetClass(ctx, alice(), roomID, "db.acme:5432", "production"); err != nil {
		t.Fatalf("raise: %v", err)
	}
	_, err = env.svc.RaiseSetupTargetClass(ctx, alice(), roomID, "db.acme:5432", "shared")
	wantSetupError(t, err, 400, "class_lowered")
	_, err = env.svc.RaiseSetupTargetClass(ctx, alice(), roomID, "db.acme:5432", "production")
	wantSetupError(t, err, 400, "class_lowered")
	_, err = env.svc.RaiseSetupTargetClass(ctx, alice(), roomID, "no.such:1", "production")
	wantSetupError(t, err, 404, "not_found")
	_, err = env.svc.RaiseSetupTargetClass(ctx, alice(), roomID, "db.acme:5432", "public")
	wantSetupError(t, err, 400, "validation")

	// Extend the provisional pipeline.
	if _, err := env.svc.ExtendSetupPipeline(ctx, alice(), roomID, room.SetupPipelineExtension{
		AddTables: []string{"public.items"}, AddConnectors: []string{"orders-sink"},
		AddConsumerGroups: []string{"orders-loader"}, AddSinkTables: []string{"public.orders_sink"},
	}); err != nil {
		t.Fatalf("extend: %v", err)
	}
	_, err = env.svc.ExtendSetupPipeline(ctx, alice(), roomID, room.SetupPipelineExtension{AddTables: []string{"public.orders"}})
	wantSetupError(t, err, 400, "validation")
	_, err = env.svc.ExtendSetupPipeline(ctx, alice(), roomID, room.SetupPipelineExtension{})
	wantSetupError(t, err, 400, "validation")
	_, err = env.svc.ExtendSetupPipeline(ctx, alice(), roomID, room.SetupPipelineExtension{AddTables: []string{"not-a-table"}})
	wantSetupError(t, err, 400, "validation")

	// The maintenance window.
	start := env.now.Add(10 * time.Minute)
	if _, err := env.svc.RecordMaintenanceWindow(ctx, alice(), roomID, start, start.Add(2*time.Hour)); err != nil {
		t.Fatalf("window: %v", err)
	}
	_, err = env.svc.RecordMaintenanceWindow(ctx, alice(), roomID, start, start.Add(5*time.Hour))
	wantSetupError(t, err, 400, "window_too_long")
	_, err = env.svc.RecordMaintenanceWindow(ctx, alice(), roomID, env.now.Add(25*time.Hour), env.now.Add(26*time.Hour))
	wantSetupError(t, err, 400, "window_too_long")
	_, err = env.svc.RecordMaintenanceWindow(ctx, alice(), roomID, start, start.Add(-time.Minute))
	wantSetupError(t, err, 400, "validation")
	_, err = env.svc.RecordMaintenanceWindow(ctx, alice(), roomID, env.now.Add(-2*time.Hour), env.now.Add(-time.Hour))
	wantSetupError(t, err, 400, "validation")

	// The snapshot folded all of it, and the scope grew.
	snap, err := env.svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Setup.Targets) != 2 {
		t.Fatalf("targets = %+v", snap.Setup.Targets)
	}
	if snap.Setup.Targets[0].Class != "production" || snap.Setup.Targets[1].Class != "shared" {
		t.Fatalf("target classes = %+v", snap.Setup.Targets)
	}
	if snap.Setup.MaintenanceWindow == nil || !snap.Setup.MaintenanceWindow.End.Equal(start.Add(2*time.Hour)) {
		t.Fatalf("window = %+v", snap.Setup.MaintenanceWindow)
	}
	pp, ok := snap.SetupProvisional()
	if !ok {
		t.Fatal("no provisional pipeline")
	}
	if len(pp.SourceTables) != 2 || pp.SourceTables[1] != "public.items" {
		t.Fatalf("source tables = %v", pp.SourceTables)
	}
	if len(pp.Connectors) != 2 || len(pp.ConsumerGroups) != 1 || len(pp.SinkTables) != 1 {
		t.Fatalf("pipeline scope = %+v", pp)
	}

	// The window is gone from the view once it has ended.
	later := room.New(room.Deps{Store: env.store, Users: mustUsers(t), Owners: env.own,
		Now:   func() time.Time { return start.Add(3 * time.Hour) },
		Setup: config.Setup{Enabled: true}, SetupResolver: fakeResolver{}})
	t.Cleanup(later.Close)
	laterView, err := later.RoomFor(ctx, alice(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if laterView.Setup.MaintenanceWindow != nil {
		t.Fatalf("window still shown after it ended: %+v", laterView.Setup.MaintenanceWindow)
	}
}

func mustUsers(t *testing.T) *room.Users {
	t.Helper()
	u, err := room.ParseUsers([]byte(setupUsers))
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestSetupCommandOnIncidentRoomAndClosedRoom(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	ctx := context.Background()

	incident, err := env.svc.CreateRoom(ctx, alice(), "incident", "sev2", "existing")
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.svc.AddSetupTarget(ctx, alice(), incident.ID, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
	wantSetupError(t, err, 400, "not_setup_room")

	res := createSetup(t, env, setupRequest("orders-setup"))
	if _, err := env.svc.CloseRoom(ctx, alice(), res.Room.ID, "abandoned"); err != nil {
		t.Fatal(err)
	}
	_, err = env.svc.AddSetupTarget(ctx, alice(), res.Room.ID, events.SetupTarget{Kind: "connect", Address: "connect.acme:8083"})
	wantSetupError(t, err, 409, "room_closed")
}

func TestSetupSnapshotFromRunnerEvents(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true, budget: 1_500_000})
	ctx := context.Background()
	res := createSetup(t, env, setupRequest("orders-setup"))
	roomID := res.Room.ID

	mustEmit := func(typ events.Type, payload any) {
		t.Helper()
		if err := env.svc.EmitSetup(ctx, roomID, typ, payload); err != nil {
			t.Fatalf("EmitSetup %s: %v", typ, err)
		}
	}
	mustEmit(events.SetupRunnerAttached, events.SetupRunnerAttachedPayload{
		RunnerLabel: "arjun-laptop", OS: "windows", Arch: "amd64",
		Tools: map[string]string{"docker": "27.1.1"}, ProjectDir: `C:\orders`,
		ProtocolVersion: 1, ProtocolMinor: 1,
	})
	mustEmit(events.SetupPlan, events.SetupPlanPayload{
		PlanID: "pl_1",
		Phases: []events.SetupPlanPhase{
			{Name: "stack", Steps: []events.SetupPlanStep{{Index: 0, Op: "compose_write"}, {Index: 1, Op: "compose_up"}}},
			{Name: "connector", Steps: []events.SetupPlanStep{{Index: 2, Op: "connect_create_connector"}}},
		},
	})
	mustEmit(events.ActionExecuted, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "ok"})
	mustEmit(events.SetupVerified, events.SetupVerifiedPayload{
		PlanID: "pl_1", Rungs: []events.SetupRung{{Name: "connector_running", OK: true}},
	})
	mustEmit(events.SetupBlocked, events.SetupBlockedPayload{Reason: events.SetupBlockedAttempts, Question: "may I retry?"})
	mustEmit(events.PipelineRegistered, events.PipelineRegisteredPayload{
		PipelineID: "orders-setup", WatchedBy: events.WatchedByServer, By: events.Actor{Kind: events.ActorHuman, ID: "alice"},
	})

	snap, err := env.svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	s := snap.Setup
	if s.Runner == nil || s.Runner.Label != "arjun-laptop" || !s.Runner.Connected || s.Runner.Tools["docker"] != "27.1.1" {
		t.Fatalf("runner = %+v", s.Runner)
	}
	if s.PlanID != "pl_1" || s.Phase != "stack" || s.StepIndex != 1 {
		t.Fatalf("plan = %q phase %q step %d; want pl_1, stack, 1", s.PlanID, s.Phase, s.StepIndex)
	}
	if len(s.Verified) != 1 || s.Verified[0].Name != "connector_running" {
		t.Fatalf("verified = %+v", s.Verified)
	}
	if s.Blocked == nil || s.Blocked.Reason != "attempts" || s.Blocked.Question != "may I retry?" {
		t.Fatalf("blocked = %+v", s.Blocked)
	}
	if s.WatchedBy != "server" {
		t.Fatalf("watched_by = %q", s.WatchedBy)
	}

	// A working agent status clears the blocked question; a lost runner stays
	// known but disconnected.
	mustEmit(events.AgentStatus, events.AgentStatusPayload{State: "working"})
	mustEmit(events.SetupRunnerLost, events.SetupRunnerLostPayload{RunnerLabel: "arjun-laptop", LastSeen: env.now})
	snap, _ = env.svc.GetRoom(ctx, roomID)
	if snap.Setup.Blocked != nil {
		t.Fatalf("blocked not cleared: %+v", snap.Setup.Blocked)
	}
	if snap.Setup.Runner == nil || snap.Setup.Runner.Connected {
		t.Fatalf("runner = %+v; want disconnected", snap.Setup.Runner)
	}

	// The documented JSON shape.
	view, err := env.svc.RoomFor(ctx, alice(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	setup, ok := m["setup"].(map[string]any)
	if !ok {
		t.Fatalf("no setup member in %s", raw)
	}
	for _, key := range []string{"mode", "runner", "plan_id", "phase", "step_index", "bounds", "targets", "provisional", "verified", "watched_by"} {
		if _, ok := setup[key]; !ok {
			t.Fatalf("setup.%s is missing: %s", key, raw)
		}
	}
	if _, ok := setup["tenant"]; ok {
		t.Fatalf("setup.tenant leaked into the JSON: %s", raw)
	}
	if setup["mode"] != true {
		t.Fatalf("setup.mode = %v", setup["mode"])
	}
	bounds, _ := setup["bounds"].(map[string]any)
	if bounds["token_budget"] != float64(1_500_000) {
		t.Fatalf("bounds = %+v", bounds)
	}
	provisional, _ := setup["provisional"].(map[string]any)
	if provisional["topic_prefix"] != "orders" {
		t.Fatalf("provisional = %+v", provisional)
	}
}

func TestSetupRoomTenantVisibility(t *testing.T) {
	env := newSetupEnv(t, setupOptions{withTenants: true})
	ctx := context.Background()
	res := createSetup(t, env, setupRequest("orders-setup"))
	roomID := res.Room.ID

	if err := env.svc.CheckRoomVisible(ctx, alice(), roomID); err != nil {
		t.Fatalf("the creator cannot see the room: %v", err)
	}
	if err := env.svc.CheckRoomVisible(ctx, bob(), roomID); err == nil {
		t.Fatal("another tenant can see the setup room")
	}
	// A setup room whose pipeline owners.yaml does not define is still in its
	// tenant's list, and its row carries only the documented fields.
	list, err := env.svc.ListRoomsFor(ctx, alice())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != roomID {
		t.Fatalf("list = %+v", list)
	}
	if list[0].Setup == nil || !list[0].Setup.Mode || list[0].Setup.Runner != nil {
		t.Fatalf("list setup = %+v", list[0].Setup)
	}
	if len(list[0].Setup.Targets) != 0 || list[0].Setup.PlanID != "" {
		t.Fatalf("list row carries more than mode/phase/runner: %+v", list[0].Setup)
	}
	other, err := env.svc.ListRoomsFor(ctx, bob())
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("another tenant's list = %+v", other)
	}
}
