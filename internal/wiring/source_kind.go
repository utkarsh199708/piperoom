package wiring

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"piperoom/internal/clients/mysql"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// The source kind of a client set (Q8g, docs/design/mysql-source.md §6.1).
//
// A CDC pipeline's source is Postgres (the default) or MySQL (owners.yaml
// source_kind). A client set has one source database URL, so every CDC
// pipeline it serves must have the same source kind, and that kind must be
// the URL's scheme: postgres:// or postgresql:// (or a libpq keyword DSN,
// which has no scheme) for postgres, mysql:// for mysql. Open refuses a set
// that disagrees before any client is built, naming the pipelines and both
// values, never a URL. The driver follows the source kind: postgres.New and
// postgres.NewWriter, or mysql.New and mysql.NewWriter. The tools and actions
// follow it too: a MySQL set registers no pg_* tool or action, a Postgres set
// no mysql_* one (agent.ToolServesSource, agent.ActionServesSource).

// sourceKindOf returns the source kind of the CDC pipelines o serves:
// postgres when it has none (a batch-only set, a Wired built by hand with no
// owners), else their one kind. Two kinds in one set is an error naming the
// pipelines of each: they would share one source database.
func sourceKindOf(o *owners.Owners) (string, error) {
	byKind := map[string][]string{}
	if o != nil {
		for i := range o.Pipelines {
			p := &o.Pipelines[i]
			if p.KindOrDefault() != events.PipelineKindCDC {
				continue
			}
			k := p.SourceKindOrDefault()
			byKind[k] = append(byKind[k], p.ID)
		}
	}
	switch len(byKind) {
	case 0:
		return events.SourceKindPostgres, nil
	case 1:
		for k := range byKind {
			return k, nil
		}
	}
	kinds := make([]string, 0, len(byKind))
	for k := range byKind {
		kinds = append(kinds, k)
	}
	slices.Sort(kinds)
	parts := make([]string, 0, len(kinds))
	for _, k := range kinds {
		parts = append(parts, fmt.Sprintf("source_kind %s (%s)", k, strings.Join(byKind[k], ", ")))
	}
	return "", fmt.Errorf("wiring: one client set serves pipelines of more than one source kind: %s; they would share one source database: give each source kind its own connections: block",
		strings.Join(parts, " and "))
}

// urlSourceKind returns the source kind a source database URL names by its
// scheme: postgres for postgres://, postgresql:// and a keyword DSN (no
// "://"), mysql for mysql://, and "" for an empty URL (nothing to judge: the
// opener reports it). Any other scheme is an error that names the scheme
// only; the rest of the URL holds the password.
func urlSourceKind(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	i := strings.Index(raw, "://")
	if i < 0 {
		return events.SourceKindPostgres, nil
	}
	switch scheme := strings.ToLower(raw[:i]); scheme {
	case "postgres", "postgresql":
		return events.SourceKindPostgres, nil
	case "mysql":
		return events.SourceKindMySQL, nil
	default:
		return "", fmt.Errorf("a %s URL (want postgres:// or mysql://)", schemeLabel(scheme))
	}
}

// schemeLabel is a scheme fit for an error: a short run of scheme characters,
// or "non-URL" for anything else, so no part of a secret can reach a log.
func schemeLabel(s string) string {
	if s == "" || len(s) > 16 {
		return "non-URL"
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '+' || r == '-' || r == '.') {
			return "non-URL"
		}
	}
	return s + "://"
}

// schemeOf is the URL form a source kind's scheme takes in an error.
func schemeOf(kind string) string {
	if kind == events.SourceKindMySQL {
		return "mysql://"
	}
	return "postgres://"
}

// checkSourceURLs returns the source kind of the set that serves o's CDC
// pipelines with cfg's source URLs, after checking that the read URL and,
// when set, the write URL name that kind (design §6.1, §12). The write URL is
// checked in both write modes, like checkSourceWriteURL. Errors name the
// pipelines, the source kind and the URL's scheme, never a URL.
func checkSourceURLs(cfg config.Config, o *owners.Owners) (string, error) {
	kind, err := sourceKindOf(o)
	if err != nil {
		return "", err
	}
	for _, u := range []struct{ name, raw string }{
		{"source database URL (SOURCE_DB_URL or connections.source_db_url)", cfg.SourceDBURL},
		{"source write URL (SOURCE_DB_WRITE_URL or connections.source_db_write_url)", cfg.SourceDBWriteURL},
	} {
		got, err := urlSourceKind(u.raw)
		if err != nil {
			return "", fmt.Errorf("wiring: %s: %s is %w", cdcPipelinesLabel(o), u.name, err)
		}
		if got != "" && got != kind {
			return "", fmt.Errorf("wiring: %s: source_kind %s but the %s is a %s URL", cdcPipelinesLabel(o), kind, u.name, schemeOf(got))
		}
	}
	return kind, nil
}

// cdcPipelinesLabel names the CDC pipelines of o for an error: "pipeline
// orders-mysql", "pipelines a, b", or "the source" without any.
func cdcPipelinesLabel(o *owners.Owners) string {
	var ids []string
	if o != nil {
		for i := range o.Pipelines {
			if o.Pipelines[i].KindOrDefault() == events.PipelineKindCDC {
				ids = append(ids, o.Pipelines[i].ID)
			}
		}
	}
	switch len(ids) {
	case 0:
		return "the source"
	case 1:
		return "pipeline " + ids[0]
	}
	return "pipelines " + strings.Join(ids, ", ")
}

// mysqlURLRole returns the user, host and port of a mysql:// URL (port 3306
// when absent), and false when it does not parse as one. The client parses
// the URL itself when it connects (and reports a bad one); this is only for
// the write-role and protected-user rules.
func mysqlURLRole(raw string) (user, host, port string, ok bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(u.Scheme, "mysql") || u.User == nil {
		return "", "", "", false
	}
	port = u.Port()
	if port == "" {
		port = "3306"
	}
	return u.User.Username(), strings.ToLower(u.Hostname()), port, true
}

// sameMySQLRole reports whether two mysql:// URLs name the same user on the
// same host and port: on MySQL an account is user@host, so another database
// or password does not make it another role.
func sameMySQLRole(read, write string) bool {
	ru, rh, rp, ok := mysqlURLRole(read)
	if !ok {
		return false
	}
	wu, wh, wp, ok := mysqlURLRole(write)
	if !ok {
		return false
	}
	return ru == wu && rh == wh && rp == wp
}

// mysqlWriterOptions are the options of the MySQL writer of a set: the read
// pool's user, so EndSession never ends a session of Piperoom's reader
// (design §6.8). A read URL that does not parse adds none: mysql.New has
// already refused it by then.
func mysqlWriterOptions(readURL string) []mysql.WriterOption {
	if user, _, _, ok := mysqlURLRole(readURL); ok && user != "" {
		return []mysql.WriterOption{mysql.WithReaderUser(user)}
	}
	return nil
}
