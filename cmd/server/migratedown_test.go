package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/migrations"
)

type fakeDownDB struct {
	plan     migrations.DownPlan
	planErr  error
	applyErr error
	planned  int // n passed to Plan
	applied  int // Apply calls
}

func (f *fakeDownDB) Plan(_ context.Context, n int) (migrations.DownPlan, error) {
	f.planned = n
	return f.plan, f.planErr
}

func (f *fakeDownDB) Apply(_ context.Context, plan migrations.DownPlan) ([]int64, error) {
	f.applied++
	if f.applyErr != nil {
		return nil, f.applyErr
	}
	var done []int64
	for _, s := range plan.Steps {
		done = append(done, s.Version)
	}
	return done, nil
}

// safePlan rolls back two migrations that keep room_events.
var safePlan = migrations.DownPlan{Current: 14, Target: 12, Steps: []migrations.DownStep{
	{Version: 14, Path: "00014_replicas.sql"}, {Version: 13, Path: "00013_spend.sql"},
}}

// eventsPlan goes all the way down, through 00001 (drops room_events).
var eventsPlan = migrations.DownPlan{Current: 2, Target: 0, Steps: []migrations.DownStep{
	{Version: 2, Path: "00002_handoff_and_steps.sql"}, {Version: 1, Path: "00001_init.sql", DropsEvents: true},
}}

func TestMigrateDownDryRunByDefault(t *testing.T) {
	for _, plan := range []migrations.DownPlan{safePlan, eventsPlan} {
		db := &fakeDownDB{plan: plan}
		var out bytes.Buffer
		// Even a matching confirmation and the second guard do nothing
		// without -migrate-down-apply.
		o := migrateDownOpts{n: len(plan.Steps), confirm: "14", dropEvents: true}
		if err := runMigrateDown(context.Background(), db, o, &out); err != nil {
			t.Fatalf("dry run: %v", err)
		}
		if db.applied != 0 {
			t.Fatalf("dry run applied %d times", db.applied)
		}
		if !strings.Contains(out.String(), "dry run, nothing changed") {
			t.Fatalf("output = %q", out.String())
		}
	}
}

func TestMigrateDownDryRunPrintsPlanAndHint(t *testing.T) {
	var out bytes.Buffer
	if err := runMigrateDown(context.Background(), &fakeDownDB{plan: eventsPlan}, migrateDownOpts{n: 2}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"database at version 2; rolling back 2 migration(s) to version 0",
		"down 1  00001_init.sql  [DELETES room_events DATA]",
		"-migrate-down 2 -migrate-down-apply -migrate-down-confirm=2 -migrate-down-drop-events",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestMigrateDownRefusals(t *testing.T) {
	tests := []struct {
		name string
		plan migrations.DownPlan
		o    migrateDownOpts
		want error
	}{
		{"zero count", safePlan, migrateDownOpts{n: 0, apply: true, confirm: "14"}, migrations.ErrDownCount},
		{"negative count", safePlan, migrateDownOpts{n: -3, apply: true, confirm: "14"}, migrations.ErrDownCount},
		{"apply without confirmation", safePlan, migrateDownOpts{n: 2, apply: true}, errDownNotConfirmed},
		{"confirmation for another version", safePlan, migrateDownOpts{n: 2, apply: true, confirm: "13"}, errDownConfirmChanged},
		{"confirmation not a number", safePlan, migrateDownOpts{n: 2, apply: true, confirm: "yes"}, errDownConfirmChanged},
		{"drops room_events without second guard", eventsPlan, migrateDownOpts{n: 2, apply: true, confirm: "2"}, errDownDropsEvents},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &fakeDownDB{plan: tt.plan}
			err := runMigrateDown(context.Background(), db, tt.o, &bytes.Buffer{})
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if db.applied != 0 {
				t.Fatalf("refused run still applied")
			}
		})
	}
}

func TestMigrateDownApplies(t *testing.T) {
	tests := []struct {
		plan migrations.DownPlan
		o    migrateDownOpts
	}{
		{safePlan, migrateDownOpts{n: 2, apply: true, confirm: "14"}},
		{eventsPlan, migrateDownOpts{n: 2, apply: true, confirm: "2", dropEvents: true}},
	}
	for _, tt := range tests {
		db := &fakeDownDB{plan: tt.plan}
		var out bytes.Buffer
		if err := runMigrateDown(context.Background(), db, tt.o, &out); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if db.applied != 1 || db.planned != tt.o.n {
			t.Fatalf("applied = %d, planned n = %d", db.applied, db.planned)
		}
		if !strings.Contains(out.String(), "done, database at version") {
			t.Fatalf("output = %q", out.String())
		}
	}
}

func TestMigrateDownErrors(t *testing.T) {
	db := &fakeDownDB{planErr: migrations.ErrDownCount}
	if err := runMigrateDown(context.Background(), db, migrateDownOpts{n: 99}, &bytes.Buffer{}); !errors.Is(err, migrations.ErrDownCount) {
		t.Fatalf("plan error: %v", err)
	}
	db = &fakeDownDB{plan: safePlan, applyErr: migrations.ErrPlanChanged}
	err := runMigrateDown(context.Background(), db, migrateDownOpts{n: 2, apply: true, confirm: "14"}, &bytes.Buffer{})
	if !errors.Is(err, migrations.ErrPlanChanged) {
		t.Fatalf("apply error: %v", err)
	}
}

// A bad database URL fails before any connection; a zero count before that.
func TestMigrateDownMainEarlyErrors(t *testing.T) {
	if err := migrateDownMain(context.Background(), "postgres://x", migrateDownOpts{n: 0}, &bytes.Buffer{}); !errors.Is(err, migrations.ErrDownCount) {
		t.Fatalf("n=0: %v", err)
	}
	if err := migrateDownMain(context.Background(), "::not a url::", migrateDownOpts{n: 1}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "parse db url") {
		t.Fatalf("bad url: %v", err)
	}
}
