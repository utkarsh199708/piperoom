// Package srcneutral tells the source-neutral tools and actions (dbtools'
// source_* tools, sourceactions; Q8n) which kind of source client they hold
// when the call carries no room pipeline.
//
// The item 2 contract (SourceSettingsReader, ScopedBlockerReader and their
// DTOs) and item 7's BinlogReader live in internal/clients/clients.go, and
// both the postgres and the mysql client alias them, so a caller asserts the
// clients interfaces on either client directly. The two kinds therefore
// share every item 2 method, and only BinlogReader, which the clients
// contract implements on MySQL alone, tells them apart.
package srcneutral

import (
	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
)

// Source kinds, as owners.yaml source_kind spells them (Q8d).
const (
	KindPostgres = "postgres"
	KindMySQL    = "mysql"
)

// KindOf reports the kind of a source client: KindMySQL for the MySQL reader
// (or any client with clients.BinlogReader, which only MySQL implements),
// KindPostgres for another client with the item 2 settings reader, "" for a
// client with neither (a fake, the replay).
func KindOf(src any) string {
	switch src.(type) {
	case *mysql.DB, clients.BinlogReader:
		return KindMySQL
	case clients.SourceSettingsReader:
		return KindPostgres
	}
	return ""
}
