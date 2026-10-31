package setupcmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/clients/confluentcloud"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
	"piperoom/internal/setup/targets/managed"
)

// fakeMSK satisfies managed.MSKAPI for registration, which calls none of
// its methods; a call would panic on the nil embedded interface.
type fakeMSK struct{ managed.MSKAPI }

type fakeConfluent struct{ managed.ConfluentAPI }

type fakeACL struct{ managed.ACLAPI }

type fakeAiven struct{ managed.AivenAPI }

// managedCalls records which client factories a registrar called.
type managedCalls struct {
	mu        sync.Mutex
	regions   []string
	confluent []string
	aiven     []string
	mskErr    error
}

func (c *managedCalls) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.regions) + len(c.confluent) + len(c.aiven)
}

// fakeManagedRegistrar is a registrar over cat whose managed clients are
// fakes, so no test reaches a cloud or this machine's AWS chain.
func fakeManagedRegistrar(t *testing.T, cat *ops.Registry) (*targetRegistrar, *managedCalls) {
	t.Helper()
	calls := &managedCalls{}
	reg := newTargetRegistrar(cat, nil)
	reg.clients = managedClients{
		msk: func(_ context.Context, region string) (managed.MSKAPI, error) {
			calls.mu.Lock()
			defer calls.mu.Unlock()
			calls.regions = append(calls.regions, region)
			if calls.mskErr != nil {
				return nil, calls.mskErr
			}
			return fakeMSK{}, nil
		},
		confluent: func(env, cluster string, _ storeOpener) (managed.ConfluentAPI, managed.ACLAPI) {
			calls.mu.Lock()
			defer calls.mu.Unlock()
			calls.confluent = append(calls.confluent, env+"/"+cluster)
			return fakeConfluent{}, fakeACL{}
		},
		aiven: func(project, service string, _ storeOpener) managed.AivenAPI {
			calls.mu.Lock()
			defer calls.mu.Unlock()
			calls.aiven = append(calls.aiven, project+"/"+service)
			return fakeAiven{}
		},
	}
	return reg, calls
}

var (
	mskOps       = []string{"msk_describe_cluster", "msk_list_plugins", managed.ReadMSKConnector, "msk_wait", "msk_create_plugin", "msk_create_connector", "msk_delete_connector"}
	confluentOps = []string{"ccloud_list_plugins", managed.ReadConfluentConnector, "ccloud_cluster", "ccloud_create_connector", "ccloud_acl_create"}
	aivenOps     = []string{"aiven_service_get", "aiven_list_plugins", managed.ReadAivenConnector, "aiven_create_connector", "aiven_integration_create"}
)

func registered(cat *ops.Registry, names []string) (in, out []string) {
	for _, n := range names {
		if _, ok := cat.Floor(n); ok {
			in = append(in, n)
		} else {
			out = append(out, n)
		}
	}
	return in, out
}

// TestRegistrarManagedKinds: each managed target kind registers its own
// provider's ops and no other's, with the client built for its address,
// and writes floor at T2 (T3 on a production target).
func TestRegistrarManagedKinds(t *testing.T) {
	cases := []struct {
		name   string
		target events.SetupTarget
		want   []string
		write  string
	}{
		{"aws", events.SetupTarget{Kind: "aws", Address: "123456789012/eu-west-1"}, mskOps, "msk_create_connector"},
		{"confluent", events.SetupTarget{Kind: "confluent", Address: "env-abc123/lkc-xyz789"}, confluentOps, "ccloud_create_connector"},
		{"aiven", events.SetupTarget{Kind: "aiven", Address: "acme-prod/orders-connect", Class: protocol.ClassProduction}, aivenOps, "aiven_create_connector"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cat := newCatalogue(t)
			reg, calls := fakeManagedRegistrar(t, cat)
			targets := []events.SetupTarget{tc.target, {Kind: "postgres", Address: "db.example.com:5432"}}
			if err := reg.Apply(context.Background(), "room-1", targets); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if _, missing := registered(cat, tc.want); len(missing) > 0 {
				t.Errorf("not registered: %v", missing)
			}
			for _, other := range [][]string{mskOps, confluentOps, aivenOps} {
				if other[0] == tc.want[0] {
					continue
				}
				if in, _ := registered(cat, other); len(in) > 0 {
					t.Errorf("another provider's ops registered: %v", in)
				}
			}
			if in, _ := registered(cat, []string{"compose_up", "service_install", "k8s_get"}); len(in) > 0 {
				t.Errorf("a local driver registered too: %v", in)
			}
			if calls.total() != 1 {
				t.Errorf("client factories called %d times, want once", calls.total())
			}
			if tc.name == "aws" && (len(calls.regions) != 1 || calls.regions[0] != "eu-west-1") {
				t.Errorf("MSK client regions = %v, want [eu-west-1]", calls.regions)
			}
			if tc.name == "confluent" && (len(calls.confluent) != 1 || calls.confluent[0] != "env-abc123/lkc-xyz789") {
				t.Errorf("Confluent client = %v", calls.confluent)
			}
			wantTier := protocol.TierT2
			if tc.target.Class == protocol.ClassProduction {
				wantTier = protocol.TierT3
			}
			if tier, ok, err := cat.FloorFor(tc.write, []byte(`{}`)); !ok || err != nil || tier != wantTier {
				t.Errorf("%s floor = %q (%v, %v), want %q", tc.write, tier, ok, err, wantTier)
			}
		})
	}
}

// TestRegistrarManagedProdPattern: PIPEROOM_SETUP_PROD_PATTERNS classes a
// managed target production on the runner as on the server.
func TestRegistrarManagedProdPattern(t *testing.T) {
	cat := newCatalogue(t)
	reg, _ := fakeManagedRegistrar(t, cat)
	reg.prodPatterns = parseProdPatterns(" 123456789012 ,")
	if err := reg.Apply(context.Background(), "room-1", []events.SetupTarget{{Kind: "aws", Address: "123456789012/eu-west-1"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if tier, _, _ := cat.FloorFor("msk_create_connector", []byte(`{}`)); tier != protocol.TierT3 {
		t.Errorf("msk_create_connector floor = %q, want T3 on a production account", tier)
	}
}

// TestRegistrarNoManagedTarget: a laptop runner whose room names no managed
// target (the compose default, a host target, a kubernetes target outside
// the in-cluster Job) registers no managed op and builds no cloud client.
func TestRegistrarNoManagedTarget(t *testing.T) {
	for name, targets := range map[string][]events.SetupTarget{
		"no targets": nil,
		"host":       {{Kind: "host", Address: "127.0.0.1"}},
		"host wins":  {{Kind: "host", Address: "127.0.0.1"}, {Kind: "aws", Address: "123456789012/eu-west-1"}},
		"kubernetes": {{Kind: "kubernetes", Address: "kind/data"}, {Kind: "kafka", Address: "strimzi://prod-kafka"}},
	} {
		t.Run(name, func(t *testing.T) {
			cat := newCatalogue(t)
			reg, calls := fakeManagedRegistrar(t, cat)
			if err := reg.Apply(context.Background(), "room-1", targets); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			all := append(append(append([]string(nil), mskOps...), confluentOps...), aivenOps...)
			if in, _ := registered(cat, all); len(in) > 0 {
				t.Errorf("managed ops registered: %v", in)
			}
			if calls.total() != 0 {
				t.Errorf("a cloud client was built %d times", calls.total())
			}
		})
	}
}

// TestRegistrarManagedErrors: a broken AWS chain fails the handshake, and a
// reconnect's Apply returns the same error without building again; two
// different accounts of one kind are refused; a bad region never reaches
// the SDK.
func TestRegistrarManagedErrors(t *testing.T) {
	cat := newCatalogue(t)
	reg, calls := fakeManagedRegistrar(t, cat)
	calls.mskErr = errors.New("no credentials")
	aws := []events.SetupTarget{{Kind: "aws", Address: "123456789012/eu-west-1"}}
	if err := reg.Apply(context.Background(), "room-1", aws); !errors.Is(err, calls.mskErr) {
		t.Fatalf("Apply = %v, want the credential error", err)
	}
	if err := reg.Apply(context.Background(), "room-1", aws); !errors.Is(err, calls.mskErr) {
		t.Fatalf("second Apply = %v, want the first error again", err)
	}
	if len(calls.regions) != 1 {
		t.Errorf("the AWS chain was loaded %d times, want once", len(calls.regions))
	}

	reg2, _ := fakeManagedRegistrar(t, newCatalogue(t))
	two := []events.SetupTarget{{Kind: "aiven", Address: "a-proj/conn"}, {Kind: "aiven", Address: "b-proj/conn"}}
	if err := reg2.Apply(context.Background(), "room-1", two); !errors.Is(err, errManagedTargets) {
		t.Fatalf("two aiven targets: Apply = %v, want errManagedTargets", err)
	}

	reg3, calls3 := fakeManagedRegistrar(t, newCatalogue(t))
	bad := []events.SetupTarget{{Kind: "aws", Address: "123456789012/evil.example.com"}}
	if err := reg3.Apply(context.Background(), "room-1", bad); err == nil {
		t.Fatal("a bad region was accepted")
	}
	if len(calls3.regions) != 0 {
		t.Errorf("a bad region reached the AWS loader: %v", calls3.regions)
	}

	reg4, _ := fakeManagedRegistrar(t, newCatalogue(t))
	if err := reg4.Apply(context.Background(), "", aws); err == nil || !strings.Contains(err.Error(), "room id") {
		t.Fatalf("an empty room id: Apply = %v, want the MSK driver's room id refusal", err)
	}
}

// openTestStore is a runner store in a fresh project directory.
func openTestStore(t *testing.T) (*runner.Store, storeOpener) {
	t.Helper()
	st, err := runner.OpenStore(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	return st, func() (*runner.Store, error) { return st, nil }
}

// TestStoreConfluentReadsTheStore: the Confluent client takes its Cloud API
// key from the runner's store at each call; a missing key names itself and
// no value; a rotated key is used at once.
func TestStoreConfluentReadsTheStore(t *testing.T) {
	var (
		mu   sync.Mutex
		seen []string
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		mu.Lock()
		seen = append(seen, user+":"+pass)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	st, open := openTestStore(t)
	c := &storeConfluent{env: "env-abc123", cluster: "lkc-xyz789", open: open, baseURL: srv.URL, transport: srv.Client().Transport}
	_, err := c.Plugins(context.Background())
	if !errors.Is(err, errNoCloudCredential) || !strings.Contains(err.Error(), secretCCloudAPIKey) {
		t.Fatalf("no key: err = %v, want errNoCloudCredential naming %s", err, secretCCloudAPIKey)
	}
	if err := st.Set(secretCCloudAPIKey, "KEYONE1234"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set(secretCCloudAPISecret, "secret-one-value"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Plugins(context.Background()); err != nil {
		t.Fatalf("Plugins: %v", err)
	}
	if err := st.Set(secretCCloudAPISecret, "secret-two-value"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Plugins(context.Background()); err != nil {
		t.Fatalf("Plugins after rotation: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != "KEYONE1234:secret-one-value" || seen[1] != "KEYONE1234:secret-two-value" {
		t.Errorf("logins = %v, want the stored key, then the rotated one", seen)
	}
}

// TestStoreACLNeedsTheClusterKey: without the cluster API key the ACL op
// gets the driver's own ErrNoACLKey, before any request.
func TestStoreACLNeedsTheClusterKey(t *testing.T) {
	_, open := openTestStore(t)
	conn := &storeConfluent{env: "env-abc123", cluster: "lkc-xyz789", open: open}
	acl := &storeACL{connect: conn, open: open}
	if err := acl.CreateACL(context.Background(), confluentACL()); !errors.Is(err, managed.ErrNoACLKey) {
		t.Fatalf("CreateACL = %v, want ErrNoACLKey", err)
	}
	var none storeOpener
	if err := (&storeACL{connect: conn, open: none}).CreateACL(context.Background(), confluentACL()); !errors.Is(err, errNoSecretStore) {
		t.Fatalf("no store: CreateACL = %v, want errNoSecretStore", err)
	}
}

func confluentACL() confluentcloud.ACL {
	return confluentcloud.ACL{ResourceType: "TOPIC", ResourceName: "orders.", PatternType: "PREFIXED",
		Principal: "User:sa-abc123", Host: "*", Operation: "READ", Permission: "ALLOW"}
}

// TestStoreAivenReadsTheStore: the Aiven token comes from the store, in the
// aivenv1 header.
func TestStoreAivenReadsTheStore(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plugins":[]}`))
	}))
	defer srv.Close()

	st, open := openTestStore(t)
	a := &storeAiven{project: "acme", service: "orders-connect", open: open, baseURL: srv.URL, transport: srv.Client().Transport}
	if _, err := a.Plugins(context.Background()); !errors.Is(err, errNoCloudCredential) || !strings.Contains(err.Error(), secretAivenToken) {
		t.Fatalf("no token: err = %v", err)
	}
	const token = "aiven-token-0123456789abcdef"
	if err := st.Set(secretAivenToken, token); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Plugins(context.Background()); err != nil {
		t.Fatalf("Plugins: %v", err)
	}
	if h := <-got; h != "aivenv1 "+token {
		t.Errorf("Authorization = %q, want the stored token", h)
	}
}

// TestStoreMaterialiser: connector configs resolve ${secret:} references
// from the runner's store; without a store the drivers get an error, never
// the reference passed through.
func TestStoreMaterialiser(t *testing.T) {
	st, open := openTestStore(t)
	if err := st.Set("db_password", "hunter2-db-pass"); err != nil {
		t.Fatal(err)
	}
	m := storeMaterialiser{open: open}
	if v, err := m.Materialise("${secret:db_password}"); err != nil || v != "hunter2-db-pass" {
		t.Fatalf("Materialise = %q, %v", v, err)
	}
	if _, err := m.Materialise("${secret:missing}"); !errors.Is(err, runner.ErrSecretMissing) {
		t.Fatalf("a missing name: %v", err)
	}
	if _, err := (storeMaterialiser{}).Materialise("${secret:db_password}"); !errors.Is(err, errNoSecretStore) {
		t.Fatalf("no store: %v", err)
	}
}

// TestStoreAdaptersRefuseCloudReferences: neither the managed drivers'
// Materialiser nor the Strimzi driver's Secret lookup hands out a cloud
// setup credential through a reference, though the managed clients
// themselves still read it (storeOpener.get).
func TestStoreAdaptersRefuseCloudReferences(t *testing.T) {
	st, open := openTestStore(t)
	if err := st.Set(secretCCloudAPISecret, "cloud-secret-value-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set("db_password", "hunter2-db-pass"); err != nil {
		t.Fatal(err)
	}
	m := storeMaterialiser{open: open}
	if v, err := m.Materialise("${secret:" + secretCCloudAPISecret + "}"); !errors.Is(err, runner.ErrCloudSecretRef) || v != "" {
		t.Fatalf("Materialise(cloud) = %q, %v; want ErrCloudSecretRef", v, err)
	}
	if v, ok := open.lookup(secretCCloudAPISecret); ok || v != "" {
		t.Fatalf("lookup(cloud) = %q, %v; want refused", v, ok)
	}
	if v, ok := open.lookup("db_password"); !ok || v != "hunter2-db-pass" {
		t.Fatalf("lookup(db_password) = %q, %v", v, ok)
	}
	if v, err := open.get(secretCCloudAPISecret); err != nil || v != "cloud-secret-value-1" {
		t.Fatalf("get(cloud) = %q, %v; the managed client must still read its key", v, err)
	}
}

// TestRegistrarRetriesAfterACancelledLoad: a cancelled or timed-out AWS load
// registered nothing, so it is not cached: the next Apply loads again and
// registers. Any other failure stays cached (TestRegistrarManagedErrors).
func TestRegistrarRetriesAfterACancelledLoad(t *testing.T) {
	cat := newCatalogue(t)
	reg, calls := fakeManagedRegistrar(t, cat)
	calls.mskErr = fmt.Errorf("setup: retrieving AWS credentials: %w", context.DeadlineExceeded)
	aws := []events.SetupTarget{{Kind: "aws", Address: "123456789012/eu-west-1"}}
	if err := reg.Apply(context.Background(), "room-1", aws); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Apply = %v, want the deadline", err)
	}
	calls.mu.Lock()
	calls.mskErr = nil
	calls.mu.Unlock()
	if err := reg.Apply(context.Background(), "room-1", aws); err != nil {
		t.Fatalf("second Apply = %v, want a fresh registration", err)
	}
	if len(calls.regions) != 2 {
		t.Errorf("the AWS chain was loaded %d times, want twice", len(calls.regions))
	}
	if in, _ := registered(cat, mskOps); len(in) != len(mskOps) {
		t.Errorf("registered %v, want every MSK op", in)
	}
}

// TestRegistrarRefusesAnotherRoom: the drivers are bound to the room they
// were registered for (they tag and guard by its id); an Apply for another
// room says to restart the runner instead of reusing them.
func TestRegistrarRefusesAnotherRoom(t *testing.T) {
	cat := newCatalogue(t)
	reg, _ := fakeManagedRegistrar(t, cat)
	aws := []events.SetupTarget{{Kind: "aws", Address: "123456789012/eu-west-1"}}
	if err := reg.Apply(context.Background(), "room-1", aws); err != nil {
		t.Fatal(err)
	}
	err := reg.Apply(context.Background(), "room-2", aws)
	if !errors.Is(err, errOtherRoom) || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("Apply(room-2) = %v, want errOtherRoom naming a restart", err)
	}
	if err := reg.Apply(context.Background(), "room-1", aws); err != nil {
		t.Fatalf("Apply(room-1) again = %v", err)
	}
}

// TestACLPrincipalFromTheStore: the Confluent driver's principal source is
// the runner's store, one ccloud_* name per template (entered on the
// terminal only); a missing one names itself.
func TestACLPrincipalFromTheStore(t *testing.T) {
	st, open := openTestStore(t)
	if _, err := open.aclPrincipal(managed.ACLTemplateWatcher); !errors.Is(err, errNoCloudCredential) || !strings.Contains(err.Error(), secretCCloudWatcherSA) {
		t.Fatalf("no entry: %v", err)
	}
	if err := st.Set(secretCCloudWatcherSA, "sa-00001"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set(secretCCloudConnectorSA, "sa-00000"); err != nil {
		t.Fatal(err)
	}
	if v, err := open.aclPrincipal(managed.ACLTemplateWatcher); err != nil || v != "sa-00001" {
		t.Fatalf("watcher = %q, %v", v, err)
	}
	if v, err := open.aclPrincipal(managed.ACLTemplateConnector); err != nil || v != "sa-00000" {
		t.Fatalf("connector = %q, %v", v, err)
	}
	if _, err := open.aclPrincipal("admin"); err == nil {
		t.Fatal("an unknown template has a principal")
	}
}
