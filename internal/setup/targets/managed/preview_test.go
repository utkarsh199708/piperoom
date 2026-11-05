package managed

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/confluentcloud"
	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// previewData decodes a preview op's Data.
func previewData(t *testing.T, res protocol.OpResult) events.PreviewResult {
	t.Helper()
	var pr events.PreviewResult
	if err := json.Unmarshal(res.Data, &pr); err != nil {
		t.Fatalf("Data %s: %v", res.Data, err)
	}
	return pr
}

// checkRollbackRecord: a reversible preview names an op of the same
// catalogue with params it validates ("<op> {json}", the room's parse); a
// preview that is not reversible gives a reason in prose.
func checkRollbackRecord(t *testing.T, reg *ops.Registry, env ops.Env, write string, pr events.PreviewResult) {
	t.Helper()
	if !pr.Reversible {
		if strings.TrimSpace(pr.Rollback) == "" {
			t.Errorf("%s: not reversible and no reason", write)
		}
		if _, rest, _ := strings.Cut(pr.Rollback, " "); strings.HasPrefix(strings.TrimSpace(rest), "{") {
			t.Errorf("%s: not reversible, but the rollback %q reads as an op and params", write, pr.Rollback)
		}
		return
	}
	op, params, _ := strings.Cut(pr.Rollback, " ")
	o, ok := reg.Get(op)
	if !ok {
		t.Errorf("%s: rollback %q names an op the catalogue lacks", write, pr.Rollback)
		return
	}
	if err := o.Validate(json.RawMessage(params), env); err != nil {
		t.Errorf("%s: rollback %q does not validate: %v", write, pr.Rollback, err)
	}
}

// Every managed write registers preview_<write> at T0.
func TestEveryManagedWriteHasAPreviewOp(t *testing.T) {
	regM, _ := newMSK(t, newFake(), "", nil)
	regC, _ := ccReg(t, &fakeCC{}, ConfluentOptions{})
	regA, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{avTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regA.Close() })
	if _, err := RegisterAiven(regA, AivenOptions{Client: &fakeAiven{}, Target: avTarget}); err != nil {
		t.Fatal(err)
	}
	for _, reg := range []*ops.Registry{regM, regC, regA} {
		for _, name := range reg.Names() {
			floor, _ := reg.Floor(name)
			if floor == protocol.TierT0 || strings.HasPrefix(name, "preview_") {
				continue
			}
			if _, core := reg.Get(name); !core {
				continue
			}
			if !strings.Contains(name, "msk_") && !strings.Contains(name, "ccloud_") && !strings.Contains(name, "aiven_") {
				continue
			}
			pf, ok := reg.Floor(PreviewName(name))
			if !ok || pf != protocol.TierT0 {
				t.Errorf("%s: preview op %s floor %q (registered %v), want T0", name, PreviewName(name), pf, ok)
			}
		}
	}
}

// MSK previews read and check but never write.
func TestMSKPreviewOps(t *testing.T) {
	f := newFake()
	reg, _ := newMSK(t, f, "", nil)
	env := ops.Env{Targets: []events.SetupTarget{mskTarget}}
	cases := map[string]map[string]any{
		"msk_create_plugin":        {"name": "piperoom-dbz", "bucket": mBucket, "key": mKey, "version": "v1", "content_type": "ZIP", "expected_size": 10},
		"msk_create_worker_config": {"name": "w", "entries": map[string]string{"key.converter": "org.apache.kafka.connect.storage.StringConverter"}},
		"msk_create_connector":     connectorParams(),
	}
	for write, params := range cases {
		res, err := exec(t, reg, PreviewName(write), params)
		if err != nil {
			t.Fatalf("%s: %v", PreviewName(write), err)
		}
		pr := previewData(t, res)
		if pr.Target == "" || pr.Expected == "" || pr.Current == "" {
			t.Errorf("%s: preview = %+v", write, pr)
		}
		checkRollbackRecord(t, reg, env, write, pr)
		if strings.Contains(string(res.Data), "pw-live-1") {
			t.Errorf("%s: a secret value reached the preview", write)
		}
	}
	if len(f.created)+len(f.workers)+len(f.plugins)+len(f.deleted) != 0 {
		t.Fatalf("a preview wrote: %d connectors, %d workers, %d plugins, %d deletes", len(f.created), len(f.workers), len(f.plugins), len(f.deleted))
	}

	// The connector preview records msk_delete_connector, and its dry run is
	// the create's own checks.
	res, _ := exec(t, reg, PreviewName("msk_create_connector"), connectorParams())
	pr := previewData(t, res)
	if !pr.Reversible || pr.Rollback != `msk_delete_connector {"name":"it_orders"}` || pr.DryRun == nil || !pr.DryRun.OK {
		t.Fatalf("connector preview = %+v (dry run %+v)", pr, pr.DryRun)
	}
	// An unsafe role is a dry run that failed, not a passing preview.
	f.trustProblem = "lets ec2.amazonaws.com assume it"
	res, _ = exec(t, reg, PreviewName("msk_create_connector"), connectorParams())
	if pr := previewData(t, res); pr.DryRun == nil || pr.DryRun.OK || !strings.Contains(pr.DryRun.Text, "ec2.amazonaws.com") {
		t.Fatalf("unsafe role: dry run %+v", pr.DryRun)
	}
}

// On production the delete is the guard's, so the create is not reversible
// by the runner and says why.
func TestMSKConnectorPreviewOnProduction(t *testing.T) {
	reg, _ := newMSK(t, newFake(), "production", nil)
	res, err := exec(t, reg, PreviewName("msk_create_connector"), connectorParams())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if pr := previewData(t, res); pr.Reversible || !strings.Contains(pr.Rollback, "person's change") {
		t.Fatalf("production preview = %+v", pr)
	}
}

// The delete's preview asks for the delete's proof: a connector this runner
// created for this room.
func TestMSKDeletePreviewNeedsTheCreatedByProof(t *testing.T) {
	f := newFake()
	reg, _ := newMSK(t, f, "", nil)
	if _, err := exec(t, reg, PreviewName("msk_delete_connector"), map[string]any{"name": "it_orders"}); !errors.Is(err, ErrNotCreatedByRoom) {
		t.Fatalf("before a create: err = %v, want ErrNotCreatedByRoom", err)
	}
	if _, err := exec(t, reg, "msk_create_connector", connectorParams()); err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err := exec(t, reg, PreviewName("msk_delete_connector"), map[string]any{"name": "it_orders"})
	if err != nil {
		t.Fatalf("after the create: %v", err)
	}
	pr := previewData(t, res)
	if pr.Reversible || !strings.Contains(pr.Expected, "deleted") || len(f.deleted) != 0 {
		t.Fatalf("delete preview = %+v, deletes %v", pr, f.deleted)
	}
	f.tags = map[string]string{TagRoom: "r_other"}
	if _, err := exec(t, reg, PreviewName("msk_delete_connector"), map[string]any{"name": "it_orders"}); !errors.Is(err, ErrNotCreatedByRoom) {
		t.Fatalf("another room's tag: err = %v", err)
	}
}

// absentMSK answers FindConnector with not found.
type absentMSK struct{ *fakeMSK }

func (absentMSK) FindConnector(context.Context, string) (string, error) {
	return "", mskconnect.ErrNotFound
}

func TestMSKConnectorPreviewReadsTheCurrentState(t *testing.T) {
	reg, _ := newMSK(t, absentMSK{newFake()}, "", nil)
	res, err := exec(t, reg, PreviewName("msk_create_connector"), connectorParams())
	if err != nil || !strings.HasPrefix(previewData(t, res).Current, "no connector named it_orders") {
		t.Fatalf("absent: %s, %v", res.Data, err)
	}
	reg, _ = newMSK(t, newFake(), "", nil)
	res, _ = exec(t, reg, PreviewName("msk_create_connector"), connectorParams())
	if !strings.Contains(previewData(t, res).Current, "the create fails as a conflict") {
		t.Fatalf("present: %s", res.Data)
	}
}

// ccPlugins is a fakeCC with a managed plugin list and a missing connector.
type ccPlugins struct{ *fakeCC }

func (ccPlugins) Plugins(context.Context) ([]confluentcloud.Plugin, error) {
	return []confluentcloud.Plugin{{Class: "PostgresCdcSourceV2", Type: "source"}}, nil
}

func (ccPlugins) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, confluentcloud.ErrNotFound
}

func TestConfluentPreviewOps(t *testing.T) {
	f := &fakeCC{}
	acl := &countingACL{}
	principal := func(string) (string, error) { return "sa-00000", nil }
	reg, _ := ccReg(t, ccPlugins{f}, ConfluentOptions{ACL: acl, Principal: principal, Secrets: fakeStore{}})
	env := ops.Env{Targets: []events.SetupTarget{ccTarget}}

	res, err := exec(t, reg, PreviewName("ccloud_create_connector"), ccParams())
	if err != nil {
		t.Fatalf("create preview: %v", err)
	}
	pr := previewData(t, res)
	if !strings.HasPrefix(pr.Current, "no connector named it_orders") || pr.DryRun != nil {
		t.Fatalf("create preview = %+v", pr)
	}
	checkRollbackRecord(t, reg, env, "ccloud_create_connector", pr)
	if len(f.validated)+len(f.created) != 0 {
		t.Fatal("the create preview validated or created: it must send no credential")
	}

	res, err = exec(t, reg, PreviewName("ccloud_acl_create"), map[string]any{"template": "connector", "principal": "User:sa-00000", "topic_prefix": "orders"})
	if err != nil {
		t.Fatalf("acl preview: %v", err)
	}
	pr = previewData(t, res)
	if !strings.Contains(pr.Expected, "ALLOW WRITE on TOPIC PREFIXED orders.") {
		t.Fatalf("acl preview = %+v", pr)
	}
	checkRollbackRecord(t, reg, env, "ccloud_acl_create", pr)
	if len(acl.rows) != 0 {
		t.Fatalf("the acl preview created %d rows", len(acl.rows))
	}

	// What the write refuses, its preview refuses.
	if _, err := exec(t, reg, PreviewName("ccloud_acl_create"), map[string]any{"template": "connector", "principal": "User:sa-99999", "topic_prefix": "orders"}); !errors.Is(err, ErrPrincipal) {
		t.Errorf("another principal: err = %v", err)
	}
	acl.exists = true
	if _, err := exec(t, reg, PreviewName("ccloud_acl_create"), map[string]any{"template": "connector", "principal": "User:sa-00000", "topic_prefix": "orders"}); !errors.Is(err, ErrTopicNotOurs) {
		t.Errorf("a bare topic that is not ours: err = %v", err)
	}
	bad := ccParams()
	bad["plugin"] = "NotAPlugin"
	if _, err := exec(t, reg, PreviewName("ccloud_create_connector"), bad); !errors.Is(err, ErrCheckFailed) {
		t.Errorf("an unknown plugin: err = %v", err)
	}
}

func TestAivenPreviewOps(t *testing.T) {
	f := &fakeAiven{}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{avTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := RegisterAiven(reg, AivenOptions{Client: f, Target: avTarget, Secrets: fakeStore{}}); err != nil {
		t.Fatal(err)
	}
	env := ops.Env{Targets: []events.SetupTarget{avTarget}}
	create := map[string]any{"name": "it_orders", "config": map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.password": "${secret:db_password}"}}
	res, err := exec(t, reg, PreviewName("aiven_create_connector"), create)
	if err != nil {
		t.Fatalf("create preview: %v", err)
	}
	checkRollbackRecord(t, reg, env, "aiven_create_connector", previewData(t, res))
	res, err = exec(t, reg, PreviewName("aiven_integration_create"), map[string]any{"source_service": "kafka-0"})
	if err != nil {
		t.Fatalf("integration preview: %v", err)
	}
	pr := previewData(t, res)
	if !strings.Contains(pr.Expected, "kafka-0 -> connect-0") {
		t.Fatalf("integration preview = %+v", pr)
	}
	checkRollbackRecord(t, reg, env, "aiven_integration_create", pr)
	if len(f.created) != 0 {
		t.Fatal("a preview created a connector")
	}
	create["config"] = map[string]string{"connector.class": "com.example.Other"}
	if _, err := exec(t, reg, PreviewName("aiven_create_connector"), create); !errors.Is(err, ErrCheckFailed) {
		t.Errorf("an unavailable plugin: err = %v", err)
	}
}

// A connector create preview refuses what the create's materialise refuses:
// no secret store, or a ${secret:} reference the store cannot resolve. The
// error names the config key, never a value.
func TestConnectorPreviewsResolveSecrets(t *testing.T) {
	missing := func(p map[string]any) map[string]any {
		cfg := map[string]string{}
		for k, v := range p["config"].(map[string]string) {
			cfg[k] = v
		}
		cfg["database.password"] = "${secret:missing}"
		p["config"] = cfg
		return p
	}

	ccNone, _ := ccReg(t, ccPlugins{&fakeCC{}}, ConfluentOptions{})
	if _, err := exec(t, ccNone, PreviewName("ccloud_create_connector"), ccParams()); !errors.Is(err, ErrNoSecrets) {
		t.Errorf("confluent, no store: err = %v, want ErrNoSecrets", err)
	}
	cc, _ := ccReg(t, ccPlugins{&fakeCC{}}, ConfluentOptions{Secrets: fakeStore{}})
	_, err := exec(t, cc, PreviewName("ccloud_create_connector"), missing(ccParams()))
	checkSecretRefusal(t, "confluent", err)

	mskReg, _ := newMSK(t, newFake(), "", nil)
	_, err = exec(t, mskReg, PreviewName("msk_create_connector"), missing(connectorParams()))
	checkSecretRefusal(t, "msk", err)

	av, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{avTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = av.Close() })
	if _, err := RegisterAiven(av, AivenOptions{Client: &fakeAiven{}, Target: avTarget, Secrets: fakeStore{}}); err != nil {
		t.Fatal(err)
	}
	create := map[string]any{"name": "it_orders", "config": map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.password": "${secret:db_password}"}}
	_, err = exec(t, av, PreviewName("aiven_create_connector"), missing(create))
	checkSecretRefusal(t, "aiven", err)
}

func checkSecretRefusal(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "database.password") {
		t.Errorf("%s, unresolvable reference: err = %v, want a refusal naming database.password", name, err)
	}
	if err != nil && strings.Contains(err.Error(), "pw-live-1") {
		t.Errorf("%s: a secret value reached the error", name)
	}
}
