package storetest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// RunConfigVersions checks the connector config history (X5c,
// clients.ConfigVersionWriter and ConfigVersionReader,
// store.ConfigVersionStore): the store computes the hash itself; a repeat of
// the newest version is no row unless it names changed keys (a change only to
// a secret value); lists are newest first without config; a hash names its
// newest version, config filled; a credential key holding a value is refused
// and nothing is written; connectors and pipelines are kept apart; at most
// store.MaxConfigVersionsPerConnector per connector; and the retention purge
// removes old versions but keeps each connector's newest. pipeline is a
// unique pipeline id for this run.
func RunConfigVersions(t *testing.T, s store.ConfigVersionStore, pipeline string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), checkTimeout)
	defer cancel()
	t0 := time.Date(2026, 10, 20, 14, 3, 0, 123456789, time.UTC)
	const conn = "pg-orders-connector"
	a := actions.RedactConfig(map[string]string{
		"connector.class":       "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "precise",
		"database.password":     "hunter2",
		// A NUL in a value: jsonb could not hold it, the text column can.
		"comment": "a\x00b",
	})
	b := maps.Clone(a)
	b["decimal.handling.mode"] = "double"
	rec := func(cfg map[string]string, at time.Time, by string, keys ...string) bool {
		t.Helper()
		ins, err := s.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{
			PipelineID: pipeline, Connector: conn, Hash: "ignored", At: at, By: by, ChangedKeys: keys, Config: cfg})
		if err != nil {
			t.Fatalf("record at %v: %v", at, err)
		}
		return ins
	}

	if !rec(a, t0, "") {
		t.Fatal("first version not inserted")
	}
	if rec(a, t0.Add(time.Minute), "") {
		t.Fatal("a repeat of the newest version was inserted")
	}
	// Same hash, but the watcher saw a secret value change: a new version.
	if !rec(a, t0.Add(2*time.Minute), "", "database.password") {
		t.Fatal("a secret-only change was not inserted")
	}
	if !rec(b, t0.Add(3*time.Minute), "proposal:p_01", "decimal.handling.mode") {
		t.Fatal("a changed config was not inserted")
	}
	// Another connector and another pipeline are separate histories.
	other := maps.Clone(b)
	if _, err := s.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{PipelineID: pipeline, Connector: "sink", At: t0, Config: other}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{PipelineID: pipeline + "x", Connector: conn, At: t0, Config: other}); err != nil {
		t.Fatal(err)
	}

	list, err := s.ConfigVersions(ctx, pipeline, conn, 10)
	if err != nil {
		t.Fatal(err)
	}
	hashA, hashB := actions.ConfigHash(a), actions.ConfigHash(b)
	wantHashes := []string{hashB, hashA, hashA}
	if len(list) != len(wantHashes) {
		t.Fatalf("list = %d versions, want %d: %+v", len(list), len(wantHashes), list)
	}
	for i, v := range list {
		if v.Hash != wantHashes[i] || v.Config != nil || v.ChangedKeys == nil || v.PipelineID != pipeline || v.Connector != conn {
			t.Fatalf("list[%d] = %+v, want hash %s, no config, changed_keys not nil", i, v, wantHashes[i])
		}
	}
	if !list[0].At.Equal(t0.Add(3*time.Minute).Truncate(time.Microsecond)) || list[0].By != "proposal:p_01" ||
		!slices.Equal(list[0].ChangedKeys, []string{"decimal.handling.mode"}) {
		t.Fatalf("newest = %+v", list[0])
	}
	if !slices.Equal(list[1].ChangedKeys, []string{events.SecretKeyChanged}) || len(list[2].ChangedKeys) != 0 {
		t.Fatalf("changed keys = %v, %v", list[1].ChangedKeys, list[2].ChangedKeys)
	}
	if got, _ := s.ConfigVersions(ctx, pipeline, conn, 1); len(got) != 1 || got[0].Hash != hashB {
		t.Fatalf("limit 1 = %+v", got)
	}
	if got, err := s.ConfigVersions(ctx, pipeline, "none", 5); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no versions = %#v, %v", got, err)
	}

	// A hash names its newest version, config filled and redacted as stored.
	v, err := s.ConfigVersion(ctx, pipeline, conn, hashA)
	if err != nil {
		t.Fatal(err)
	}
	if !v.At.Equal(t0.Add(2*time.Minute).Truncate(time.Microsecond)) || v.Hash != hashA || !maps.Equal(v.Config, a) {
		t.Fatalf("version %s = %+v", hashA, v)
	}
	if v.Config["database.password"] != actions.RedactedValue {
		t.Fatalf("stored password = %q", v.Config["database.password"])
	}
	if _, err := s.ConfigVersion(ctx, pipeline, conn, strings.Repeat("0", 64)); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown hash: %v", err)
	}
	if _, err := s.ConfigVersion(ctx, pipeline, "sink", hashA); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("another connector's hash: %v", err)
	}

	// Refused before any write: a credential key with a value, and bad input.
	raw := maps.Clone(a)
	raw["database.password"] = "hunter2"
	if _, err := s.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{PipelineID: pipeline, Connector: conn, At: t0.Add(time.Hour), Config: raw}); !errors.Is(err, store.ErrSecretValue) || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("secret value: %v", err)
	}
	tooMany := map[string]string{}
	for i := range clients.MaxConfigKeys + 1 {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	long := map[string]string{"x": strings.Repeat("a", actions.MaxConfigValueBytes+1)}
	good := clients.ConnectorConfigVersion{PipelineID: pipeline, Connector: conn, At: t0.Add(time.Hour), Config: b}
	for name, mut := range map[string]func(*clients.ConnectorConfigVersion){
		"pipeline":  func(v *clients.ConnectorConfigVersion) { v.PipelineID = "" },
		"connector": func(v *clients.ConnectorConfigVersion) { v.Connector = "" },
		"at":        func(v *clients.ConnectorConfigVersion) { v.At = time.Time{} },
		"by":        func(v *clients.ConnectorConfigVersion) { v.By = "alice" },
		"by empty":  func(v *clients.ConnectorConfigVersion) { v.By = "proposal:" },
		"keys":      func(v *clients.ConnectorConfigVersion) { v.Config = tooMany },
		"changed":   func(v *clients.ConnectorConfigVersion) { v.ChangedKeys = slices.Collect(maps.Keys(tooMany)) },
		"long":      func(v *clients.ConnectorConfigVersion) { v.Config = long },
		"nul":       func(v *clients.ConnectorConfigVersion) { v.Connector = "a\x00b" },
	} {
		bad := good
		mut(&bad)
		if ins, err := s.RecordConfigVersion(ctx, bad); err == nil || ins {
			t.Errorf("bad %s: inserted %v, err %v", name, ins, err)
		}
	}
	for _, n := range []int{0, -1, clients.MaxConfigVersionList + 1} {
		if _, err := s.ConfigVersions(ctx, pipeline, conn, n); !errors.Is(err, store.ErrConfigVersion) {
			t.Errorf("limit %d: %v", n, err)
		}
	}
	if got, _ := s.ConfigVersions(ctx, pipeline, conn, 10); len(got) != 3 {
		t.Fatalf("a refused write changed the list: %d versions", len(got))
	}

	// Changed key names: sorted, no repeats, credential names masked.
	if !rec(a, t0.Add(4*time.Minute), "", "z.key", "a.mode", "a.mode", "ssl.truststore.password") {
		t.Fatal("not inserted")
	}
	if got, _ := s.ConfigVersions(ctx, pipeline, conn, 1); !slices.Equal(got[0].ChangedKeys, []string{events.SecretKeyChanged, "a.mode"}) {
		t.Fatalf("changed keys = %v", got[0].ChangedKeys)
	}

	// Cap: the newest MaxConfigVersionsPerConnector of a connector stay.
	capConn := "flapping"
	n := store.MaxConfigVersionsPerConnector + 3
	for i := range n {
		cfg := map[string]string{"n": fmt.Sprint(i)}
		if _, err := s.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{PipelineID: pipeline, Connector: capConn, At: t0.Add(time.Duration(i) * time.Second), Config: cfg, ChangedKeys: []string{"n"}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ConfigVersions(ctx, pipeline, capConn, clients.MaxConfigVersionList)
	if err != nil || len(got) != clients.MaxConfigVersionList {
		t.Fatalf("capped list: %d, %v", len(got), err)
	}
	if _, err := s.ConfigVersion(ctx, pipeline, capConn, actions.ConfigHash(map[string]string{"n": "2"})); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("a version beyond the cap is still there: %v", err)
	}
	if _, err := s.ConfigVersion(ctx, pipeline, capConn, actions.ConfigHash(map[string]string{"n": "3"})); err != nil {
		t.Fatalf("the oldest kept version: %v", err)
	}

	// Retention: older than the cutoff goes, but each connector keeps its
	// newest version (sink has only an old one).
	oldConn := "old-only"
	for i, cfg := range []map[string]string{{"v": "1"}, {"v": "2"}} {
		if _, err := s.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{PipelineID: pipeline, Connector: oldConn, At: t0.AddDate(0, 0, -40).Add(time.Duration(i) * time.Hour), Config: cfg}); err != nil {
			t.Fatal(err)
		}
	}
	purged, err := s.PurgeConfigVersionsBefore(ctx, t0.Add(150*time.Second).Truncate(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	// conn: the versions at t0 and t0+2m; flapping: t0+0s..t0+149s that the
	// cap left (3..149); old-only: its older one. sink and the other
	// pipeline keep their only version.
	if want := int64(2 + 147 + 1); purged != want {
		t.Fatalf("purged %d, want %d", purged, want)
	}
	if got, _ := s.ConfigVersions(ctx, pipeline, conn, 10); len(got) != 2 || got[1].Hash != hashB {
		t.Fatalf("after purge = %+v", got)
	}
	if got, _ := s.ConfigVersions(ctx, pipeline, oldConn, 10); len(got) != 1 || got[0].Hash != actions.ConfigHash(map[string]string{"v": "2"}) {
		t.Fatalf("old-only after purge = %+v", got)
	}
	if got, _ := s.ConfigVersions(ctx, pipeline, "sink", 10); len(got) != 1 {
		t.Fatalf("sink after purge = %+v", got)
	}
}
