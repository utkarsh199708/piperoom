package changes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// MigrationReader reads the Flyway and Liquibase history tables of the source
// database (X4b). The source Postgres client (*postgres.DB) implements it; it
// is asserted on Deps.Src at use time, so a source without it only has no
// migrations. It is local until clients gains a source-neutral migration
// reader (contract change request in the X4b PR).
type MigrationReader interface {
	RecentMigrations(ctx context.Context, schemas []string, since time.Time, limit int) (postgres.MigrationHistory, error)
}

var _ MigrationReader = (*postgres.DB)(nil)

// readMigrations returns the migration changes of the room's pipeline at or
// after since: Flyway and Liquibase history tables in the schemas of the
// pipeline's source tables (a cdc pipeline), and the latest dbt run of the
// pipeline's models (a batch pipeline, when a dbt reader is configured). ok
// reports whether at least one reader answered; notes are the facts for the
// summary (an unreadable history table, a failed read). err is only the
// caller's context ending.
func (t *Tool) readMigrations(ctx context.Context, sc tools.Scope, since time.Time) (out []clients.PipelineChange, notes []string, ok bool, err error) {
	pipeline := sc.Pipeline().ID
	if mr, isMR := t.d.Src.(MigrationReader); isMR {
		if schemas := sourceSchemas(sc); len(schemas) > 0 {
			cctx, cancel := context.WithTimeout(ctx, callTimeout)
			h, rerr := mr.RecentMigrations(cctx, schemas, since, min(listLimit, postgres.MaxMigrations))
			cancel()
			if rerr != nil {
				if ctx.Err() != nil {
					return nil, nil, false, ctx.Err()
				}
				t.d.Logger.Warn("what_changed: migration history read failed", "pipeline", pipeline, "err", rerr)
				notes = append(notes, "migrations: live read failed")
			} else {
				ok = true
				for _, tb := range h.Tables {
					switch tb.State {
					case postgres.HistoryUnreadable:
						notes = append(notes, "migration history not readable (grant SELECT on "+grantName(tb)+")")
					case postgres.HistoryUnknownShape:
						notes = append(notes, cleanLine(tb.Qualified(), 120)+" is not a "+tb.Source+" history table (columns differ); skipped")
					}
				}
				dropped := 0
				for _, m := range h.Migrations {
					c, valid := migrationChange(pipeline, m)
					if !valid {
						dropped++
						continue
					}
					out = append(out, c)
				}
				if dropped > 0 {
					notes = append(notes, fmt.Sprintf("%d migration %s dropped (version over %d bytes or control characters)",
						dropped, tools.Plural(dropped, "row", "rows"), clients.MaxExternalChangeVersionBytes))
				}
			}
		}
	}

	if t.d.Transform != nil && len(sc.Pipeline().Models) > 0 {
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		rr, rerr := t.d.Transform.RunResults(cctx)
		cancel()
		switch {
		case errors.Is(rerr, clients.ErrNotFound): // no run yet
			ok = true
		case rerr != nil:
			if ctx.Err() != nil {
				return nil, nil, false, ctx.Err()
			}
			t.d.Logger.Warn("what_changed: dbt run results read failed", "pipeline", pipeline, "err", rerr)
			notes = append(notes, "dbt: run results read failed")
		default:
			ok = true
			if c, found := dbtChange(pipeline, sc, rr); found && !c.At.Before(since) {
				out = append(out, c)
			}
		}
	}
	return out, notes, ok, nil
}

// sourceSchemas returns the distinct schemas of the pipeline's source tables,
// sorted. Only those are searched: a history table in another schema of a
// shared database may be another team's.
func sourceSchemas(sc tools.Scope) []string {
	var out []string
	for _, tbl := range sc.Pipeline().SourceTables {
		if parts, ok := splitIdent(tbl, false); ok && len(parts) == 2 && !slices.Contains(out, parts[0]) {
			out = append(out, parts[0])
		}
	}
	sort.Strings(out)
	return out
}

// grantName is the table to name in the grant fact: bare in public, where
// both tools put it by default, else schema-qualified.
func grantName(h postgres.HistoryTable) string {
	if h.Schema == "public" {
		return h.Name
	}
	return cleanLine(h.Qualified(), 120)
}

// migrationChange converts a history row to the change form of docs/api.md
// (External changes): Object the history table, Actor installed_by or author,
// After {version}, Summary the description. A row whose version fails
// ExternalChange.Validate is dropped, never fixed up.
func migrationChange(pipeline string, m postgres.Migration) (clients.PipelineChange, bool) {
	ext := clients.ExternalChange{Version: m.Version}
	if ext.Validate() != nil {
		return clients.PipelineChange{}, false
	}
	after, err := json.Marshal(ext)
	if err != nil {
		return clients.PipelineChange{}, false
	}
	summary := m.Description
	if !m.Success {
		summary = "FAILED " + summary
	}
	c := clients.PipelineChange{
		PipelineID: pipeline,
		Kind:       clients.ChangeMigration,
		Object:     m.Table,
		At:         m.InstalledOn.UTC(),
		Actor:      cleanLine(m.InstalledBy, 128),
		Source:     m.Source,
		After:      after,
		Summary:    cleanLine(summary, maxLineRunes),
	}
	c.Fingerprint = changeFingerprint(c)
	return c, true
}

// dbtPackageRe is a dbt package name as unique_ids carry it.
var dbtPackageRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// dbtStatuses are the run statuses dbt reports; any other value is "other",
// so a hostile run_results.json cannot put text in the summary.
var dbtStatuses = []string{"error", "fail", "pass", "skipped", "success", "warn"}

// dbtChange turns the latest dbt run into one migration change when it ran
// at least one of the pipeline's models. Counts per status only: dbt's
// messages may quote SQL or data and are never read here.
func dbtChange(pipeline string, sc tools.Scope, rr clients.RunResults) (clients.PipelineChange, bool) {
	if rr.GeneratedAt.IsZero() {
		return clients.PipelineChange{}, false
	}
	counts := map[string]int{}
	n, project := 0, ""
	for _, r := range rr.Results {
		if sc.CheckModel(r.Model) != nil {
			continue
		}
		n++
		st := r.Status
		if !slices.Contains(dbtStatuses, st) {
			st = "other"
		}
		counts[st]++
		if parts := strings.Split(r.Model, "."); project == "" && len(parts) >= 3 && parts[0] == "model" && dbtPackageRe.MatchString(parts[1]) {
			project = parts[1]
		}
	}
	if n == 0 {
		return clients.PipelineChange{}, false
	}
	if project == "" {
		project = "dbt"
	}
	var parts []string
	for _, st := range append(slices.Clone(dbtStatuses), "other") {
		if counts[st] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[st], st))
		}
	}
	c := clients.PipelineChange{
		PipelineID: pipeline,
		Kind:       clients.ChangeMigration,
		Object:     project,
		At:         rr.GeneratedAt.UTC(),
		Source:     clients.ChangeSourceDBT,
		After:      json.RawMessage(`{}`),
		Summary:    fmt.Sprintf("dbt run: %d %s of this pipeline (%s)", n, tools.Plural(n, "model", "models"), strings.Join(parts, ", ")),
	}
	c.Fingerprint = changeFingerprint(c)
	return c, true
}

// changeFingerprint is the documented dedup key of clients.PipelineChange:
// sha256 of kind|object|at|before|after.
func changeFingerprint(c clients.PipelineChange) string {
	h := sha256.Sum256([]byte(c.Kind + "|" + c.Object + "|" + c.At.UTC().Format(time.RFC3339Nano) + "|" + string(c.Before) + "|" + string(c.After)))
	return hex.EncodeToString(h[:])
}

// migrationVersion is the version of a migration change's After, "" when
// none or unreadable.
func migrationVersion(c clients.PipelineChange) string {
	var e clients.ExternalChange
	if json.Unmarshal(c.After, &e) != nil {
		return ""
	}
	return e.Version
}
