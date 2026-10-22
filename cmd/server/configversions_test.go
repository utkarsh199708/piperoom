package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// The room store holds both tables, as the server wires it.
var _ configVersionsPurger = (*memstore.Store)(nil)
var _ configVersionsPurger = (*store.PG)(nil)

// X5c TC3: the change-log retention also removes config versions older than
// PIPEROOM_CHANGES_RETENTION_DAYS (fake clock), but keeps each connector's
// newest version so the current config stays diffable.
func TestChangesRetentionPurgesConfigVersions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	st := memstore.New()
	rec := func(conn string, at time.Time, v string) {
		t.Helper()
		if _, err := st.RecordConfigVersion(ctx, clients.ConnectorConfigVersion{
			PipelineID: "orders", Connector: conn, At: at, Config: map[string]string{"v": v}}); err != nil {
			t.Fatal(err)
		}
	}
	rec("c1", now.AddDate(0, 0, -40), "1")
	rec("c1", now.AddDate(0, 0, -31), "2")
	rec("c1", now.AddDate(0, 0, -29), "3")
	rec("old", now.AddDate(0, 0, -50), "1")
	rec("old", now.AddDate(0, 0, -45), "2")

	var buf bytes.Buffer
	runChangesRetention(ctx, st, 30, time.Second, func() time.Time { return now }, slog.New(slog.NewTextHandler(&buf, nil)))
	if !strings.Contains(buf.String(), "config versions retention: deleted old rows") || !strings.Contains(buf.String(), "rows=3") {
		t.Fatalf("log: %s", buf.String())
	}
	if got, _ := st.ConfigVersions(ctx, "orders", "c1", 10); len(got) != 1 || !got[0].At.Equal(now.AddDate(0, 0, -29)) {
		t.Fatalf("c1 after retention = %+v", got)
	}
	if got, _ := st.ConfigVersions(ctx, "orders", "old", 10); len(got) != 1 || !got[0].At.Equal(now.AddDate(0, 0, -45)) {
		t.Fatalf("old after retention = %+v (its newest must stay)", got)
	}
}

// failingVersions purges changes but fails on config versions.
type failingVersions struct{ fakeChangesPurger }

func (*failingVersions) PurgeConfigVersionsBefore(context.Context, time.Time) (int64, error) {
	return 0, errors.New("versions table gone")
}

// A failed config-version purge is logged under its own name and does not
// hide the change-log purge's outcome.
func TestChangesRetentionConfigVersionsError(t *testing.T) {
	p := &failingVersions{fakeChangesPurger{n: 2}}
	var buf bytes.Buffer
	runChangesRetention(context.Background(), p, 30, time.Second, time.Now, slog.New(slog.NewTextHandler(&buf, nil)))
	if !strings.Contains(buf.String(), "changes retention: deleted old rows") ||
		!strings.Contains(buf.String(), "config versions retention: purge failed") {
		t.Fatalf("log: %s", buf.String())
	}
}
