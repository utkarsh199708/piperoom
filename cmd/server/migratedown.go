package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"piperoom/migrations"
)

// migrateDownTimeout bounds a whole rollback, lock wait included. Down
// migrations drop tables and columns; they are quick, but a lock held by a
// starting replica may need a moment.
const migrateDownTimeout = 5 * time.Minute

// migrateDownOpts are the -migrate-down flags. Without apply the run is a dry
// run: it prints the plan and changes nothing.
type migrateDownOpts struct {
	n     int
	apply bool
	// confirm must be the database's current version, exactly as the dry run
	// printed it: it proves the operator read that plan, and a database that
	// moved since then is refused.
	confirm string
	// dropEvents is the second guard: a plan that loses room_events data (the
	// incident log, which nothing can rebuild) is refused without it.
	dropEvents bool
}

var (
	errDownNotConfirmed   = errors.New("server: -migrate-down-apply needs -migrate-down-confirm=<current version from the dry run>")
	errDownConfirmChanged = errors.New("server: -migrate-down-confirm does not match the database's current version; run the dry run again")
	errDownDropsEvents    = errors.New("server: this rollback deletes room_events data (the incident log); add -migrate-down-drop-events to allow it")
	errDownStrayFlag      = errors.New("server: -migrate-down-apply, -migrate-down-confirm and -migrate-down-drop-events need -migrate-down N")
)

// downDB plans and applies rollbacks; pgDownDB is the real one.
type downDB interface {
	Plan(ctx context.Context, n int) (migrations.DownPlan, error)
	// Apply takes the migration lock, checks the database still matches plan
	// and rolls it back.
	Apply(ctx context.Context, plan migrations.DownPlan) ([]int64, error)
}

// checkDownGuards returns why plan may not run with o, or nil.
func checkDownGuards(o migrateDownOpts, plan migrations.DownPlan) error {
	if o.confirm == "" {
		return errDownNotConfirmed
	}
	if o.confirm != strconv.FormatInt(plan.Current, 10) {
		return errDownConfirmChanged
	}
	if plan.DropsEvents() && !o.dropEvents {
		return errDownDropsEvents
	}
	return nil
}

// runMigrateDown prints the plan for rolling back o.n migrations and, only
// with apply, a matching confirmation and (when room_events data would go)
// the second guard, applies it.
func runMigrateDown(ctx context.Context, db downDB, o migrateDownOpts, out io.Writer) error {
	if o.n < 1 {
		return fmt.Errorf("server: -migrate-down %d: %w", o.n, migrations.ErrDownCount)
	}
	plan, err := db.Plan(ctx, o.n)
	if err != nil {
		return fmt.Errorf("server: plan rollback: %w", err)
	}
	fmt.Fprintf(out, "migrate-down: database at version %d; rolling back %d migration(s) to version %d:\n", plan.Current, len(plan.Steps), plan.Target)
	for _, s := range plan.Steps {
		note := ""
		if s.DropsEvents {
			note = "  [DELETES room_events DATA]"
		}
		fmt.Fprintf(out, "  down %d  %s%s\n", s.Version, s.Path, note)
	}
	if !o.apply {
		hint := fmt.Sprintf("-migrate-down %d -migrate-down-apply -migrate-down-confirm=%d", o.n, plan.Current)
		if plan.DropsEvents() {
			hint += " -migrate-down-drop-events"
		}
		fmt.Fprintf(out, "migrate-down: dry run, nothing changed. To apply: %s\n", hint)
		return nil
	}
	if err := checkDownGuards(o, plan); err != nil {
		return err
	}
	done, err := db.Apply(ctx, plan)
	for _, v := range done {
		fmt.Fprintf(out, "migrate-down: rolled back %d\n", v)
	}
	if err != nil {
		return fmt.Errorf("server: rollback stopped after %d of %d: %w", len(done), len(plan.Steps), err)
	}
	fmt.Fprintf(out, "migrate-down: done, database at version %d\n", plan.Target)
	return nil
}

// pgDownDB is downDB over the room database.
type pgDownDB struct{ db *sql.DB }

func (p pgDownDB) Plan(ctx context.Context, n int) (migrations.DownPlan, error) {
	return migrations.PlanDown(ctx, p.db, n)
}

func (p pgDownDB) Apply(ctx context.Context, plan migrations.DownPlan) ([]int64, error) {
	var done []int64
	// Same lock as start-up migrations, so no replica migrates up meanwhile;
	// ApplyDown re-plans under it.
	err := withMigrationLock(ctx, p.db, func() error {
		var err error
		done, err = migrations.ApplyDown(ctx, p.db, plan)
		return err
	})
	return done, err
}

// migrateDownMain runs -migrate-down against dbURL and returns; the server
// does not start.
func migrateDownMain(ctx context.Context, dbURL string, o migrateDownOpts, out io.Writer) error {
	if o.n < 1 {
		return fmt.Errorf("server: -migrate-down %d: %w", o.n, migrations.ErrDownCount)
	}
	cc, err := pgx.ParseConfig(dbURL)
	if err != nil {
		return fmt.Errorf("server: parse db url: %w", err)
	}
	db := stdlib.OpenDB(*cc)
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, migrateDownTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("server: ping for migrate-down: %w", err)
	}
	return runMigrateDown(ctx, pgDownDB{db: db}, o, out)
}
