package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"

	"github.com/pressly/goose/v3"
)

// DownStep is one applied migration a rollback would undo.
type DownStep struct {
	Version int64
	Path    string
	// DropsEvents is true when the migration's Down section deletes room_events
	// rows or the table itself: the incident log, which nothing can rebuild.
	DropsEvents bool
}

// DownPlan is what rolling back N migrations would do, newest first.
type DownPlan struct {
	Current int64 // the database version now
	Target  int64 // the version after the rollback (0: nothing applied)
	Steps   []DownStep
}

// DropsEvents reports whether any step loses room_events data.
func (p DownPlan) DropsEvents() bool {
	return slices.ContainsFunc(p.Steps, func(s DownStep) bool { return s.DropsEvents })
}

// ErrDownCount is returned for a rollback count below 1 or above the number of
// applied migrations.
var ErrDownCount = errors.New("migrations: bad rollback count")

// ErrPlanChanged is returned when the database is no longer in the state the
// plan was made for (another process migrated in between), or goose rolled
// back a different version than planned.
var ErrPlanChanged = errors.New("migrations: database changed since the plan was made")

// downProvider is the part of *goose.Provider a rollback uses, so plans and
// their refusals are tested without a database.
type downProvider interface {
	Status(ctx context.Context) ([]*goose.MigrationStatus, error)
	Down(ctx context.Context) (*goose.MigrationResult, error)
}

// PlanDown reports what rolling back the n most recent migrations of db
// would do. It changes nothing.
func PlanDown(ctx context.Context, db *sql.DB, n int) (DownPlan, error) {
	p, err := goose.NewProvider(goose.DialectPostgres, db, FS)
	if err != nil {
		return DownPlan{}, fmt.Errorf("migrations: build provider: %w", err)
	}
	return planDown(ctx, p, FS, n)
}

// ApplyDown rolls back exactly the steps of plan. It first re-plans and
// refuses with ErrPlanChanged unless the database is still where plan found
// it, then stops at the first step goose fails or runs out of order. It
// returns the versions it rolled back, also on error. The caller holds the
// migration lock.
func ApplyDown(ctx context.Context, db *sql.DB, plan DownPlan) ([]int64, error) {
	p, err := goose.NewProvider(goose.DialectPostgres, db, FS)
	if err != nil {
		return nil, fmt.Errorf("migrations: build provider: %w", err)
	}
	return applyDown(ctx, p, FS, plan)
}

func planDown(ctx context.Context, p downProvider, fsys fs.FS, n int) (DownPlan, error) {
	if n < 1 {
		return DownPlan{}, fmt.Errorf("%w: %d (want at least 1)", ErrDownCount, n)
	}
	status, err := p.Status(ctx)
	if err != nil {
		return DownPlan{}, fmt.Errorf("migrations: status: %w", err)
	}
	var applied []*goose.Source
	for _, s := range status {
		if s.State == goose.StateApplied && s.Source != nil {
			applied = append(applied, s.Source)
		}
	}
	slices.SortFunc(applied, func(a, b *goose.Source) int { return int(b.Version - a.Version) })
	if n > len(applied) {
		return DownPlan{}, fmt.Errorf("%w: %d, but only %d migrations are applied", ErrDownCount, n, len(applied))
	}
	plan := DownPlan{Current: applied[0].Version}
	if n < len(applied) {
		plan.Target = applied[n].Version
	}
	for _, src := range applied[:n] {
		drops, err := downDropsEvents(fsys, src.Path)
		if err != nil {
			return DownPlan{}, err
		}
		plan.Steps = append(plan.Steps, DownStep{Version: src.Version, Path: src.Path, DropsEvents: drops})
	}
	return plan, nil
}

func applyDown(ctx context.Context, p downProvider, fsys fs.FS, plan DownPlan) ([]int64, error) {
	now, err := planDown(ctx, p, fsys, len(plan.Steps))
	if err != nil {
		return nil, err
	}
	if !samePlan(now, plan) {
		return nil, fmt.Errorf("%w: planned %d -> %d, database now at %d", ErrPlanChanged, plan.Current, plan.Target, now.Current)
	}
	var done []int64
	for _, step := range plan.Steps {
		res, err := p.Down(ctx)
		if err != nil {
			return done, fmt.Errorf("migrations: down %d: %w", step.Version, err)
		}
		if res == nil || res.Source == nil || res.Source.Version != step.Version {
			got := int64(-1)
			if res != nil && res.Source != nil {
				got = res.Source.Version
				done = append(done, got)
			}
			return done, fmt.Errorf("%w: rolled back %d, planned %d", ErrPlanChanged, got, step.Version)
		}
		done = append(done, step.Version)
	}
	return done, nil
}

func samePlan(a, b DownPlan) bool {
	return a.Current == b.Current && a.Target == b.Target &&
		slices.EqualFunc(a.Steps, b.Steps, func(x, y DownStep) bool { return x == y })
}

// eventLossRe matches the Down statements that lose room_events data: dropping
// the table, dropping one of its columns, truncating or deleting from it.
// \b keeps index names such as room_events_room_seq_idx out.
var eventLossRe = regexp.MustCompile(`(?is)\b(?:drop\s+table\b[^;]*\broom_events\b|alter\s+table\s+(?:only\s+)?(?:if\s+exists\s+)?room_events\b[^;]*\bdrop\b|truncate\b[^;]*\broom_events\b|delete\s+from\s+room_events\b)`)

// downDropsEvents reads the "-- +goose Down" section of the migration at path
// and reports whether it loses room_events data. Reading the SQL, not a list
// kept by hand, means a new migration is classified without anyone
// remembering to.
func downDropsEvents(fsys fs.FS, path string) (bool, error) {
	b, err := fs.ReadFile(fsys, path)
	if err != nil {
		return false, fmt.Errorf("migrations: read %s: %w", path, err)
	}
	_, down, ok := strings.Cut(string(b), "-- +goose Down")
	if !ok {
		return false, nil
	}
	return eventLossRe.MatchString(stripSQLComments(down)), nil
}

// stripSQLComments drops "--" line comments so a comment that mentions the
// table does not count as a statement.
func stripSQLComments(s string) string {
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if j := strings.Index(l, "--"); j >= 0 {
			lines[i] = l[:j]
		}
	}
	return strings.Join(lines, "\n")
}
