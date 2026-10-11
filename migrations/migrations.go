// Package migrations embeds the SQL migration files so tests and tools can
// apply them without the goose CLI.
package migrations

import "embed"

// FS holds every *.sql migration in this directory.
//
//go:embed *.sql
var FS embed.FS
