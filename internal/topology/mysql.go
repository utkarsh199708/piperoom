package topology

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
)

// This file holds the source node of a MySQL pipeline (Q8d,
// docs/design/mysql-source.md §9): owners.yaml source_kind: mysql gives the
// node id "mysql:<db>.<table>" in layer mysql, with facts from the server's
// binlog state instead of a replication slot (MySQL has none).
//
// BinlogReader, BinlogStatus and BinlogFile are the Q0 item 7 contract items
// of design §5.2 (clients.BinlogReader and friends), not in internal/clients
// yet. Until they land there they are aliases of the MySQL client's own
// declarations, so the builder's type assertion on the source client sees
// exactly the method *mysql.DB has (a mirrored struct of the same shape is a
// different type, and the assertion would never match). The contract PR
// re-points the aliases at clients. The builder asserts it on the source
// client and never calls ReplicationSlots for a MySQL pipeline.

// BinlogReader is the optional binlog reader of a MySQL source client. Read
// only; 10 s per call.
type BinlogReader = mysql.BinlogReader

// BinlogStatus is the binlog state of a MySQL server: positions and names
// only, never event contents (design §5.2).
type BinlogStatus = mysql.BinlogStatus

// BinlogFile is one binlog file the server still has.
type BinlogFile = mysql.BinlogFile

// The MySQL client is the source client the builder asserts BinlogReader
// (and pkColumner, for a trace's key) on: if either method set drifts, the
// build fails here instead of the binlog node or the trace's key silently
// falling back.
var (
	_ BinlogReader = (*mysql.DB)(nil)
	_ pkColumner   = (*mysql.DB)(nil)
)

// Node id prefixes of a source table, by source kind (graph.go's Node doc
// lists pg:; the mysql: prefix is the doc-only item of design §5.4).
const (
	pgSourcePrefix    = "pg:"
	mysqlSourcePrefix = "mysql:"
)

// sourceNodeID is the graph id of a source table of p: "pg:<schema>.<table>"
// or, for a MySQL pipeline, "mysql:<db>.<table>". Every edge, lineage and
// PII rule that names a source table goes through it, so the ids agree.
func sourceNodeID(p owners.Pipeline, tbl string) string {
	if p.IsMySQL() {
		return mysqlSourcePrefix + tbl
	}
	return pgSourcePrefix + tbl
}

// sourceTableOf returns the table of a source node id, of either kind.
func sourceTableOf(id string) string {
	if t, ok := strings.CutPrefix(id, mysqlSourcePrefix); ok {
		return t
	}
	return strings.TrimPrefix(id, pgSourcePrefix)
}

// errNoBinlogReader: the source client of a MySQL pipeline cannot read the
// binlog state (no BinlogReader), so the node's facts are unknown.
var errNoBinlogReader = errors.New("topology: the source client has no binlog reader")

// collectBinlog reads the binlog state of a MySQL pipeline's source, or
// records why it cannot.
func (b *builder) collectBinlog(ctx context.Context) (*BinlogStatus, error) {
	br, ok := b.src.(BinlogReader)
	if !ok {
		return nil, errNoBinlogReader
	}
	st, err := br.BinlogStatus(ctx)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// mysqlSourceNode is the source node of a MySQL pipeline's table. Its
// health and facts describe the server's binlog, which every table of the
// pipeline shares: a failed read is "error: unreachable" as for Postgres; a
// client without a binlog reader (or one answering ErrNotSupported) leaves
// "binlog: unknown".
func (b *builder) mysqlSourceNode(p owners.Pipeline, tbl string, info *collected) Node {
	layer := p.SourceLayer()
	n := Node{
		ID:     sourceNodeID(p, tbl),
		Kind:   KindSourceTable,
		Name:   tbl,
		Layer:  layer,
		Owner:  b.layerOwner(layer),
		Health: HealthUnknown,
	}
	switch err := info.binlogErr; {
	case errors.Is(err, errNoBinlogReader), errors.Is(err, clients.ErrNotSupported):
		n.Facts = map[string]string{"binlog": "unknown"}
		return n
	case err != nil:
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	case info.binlog == nil:
		n.Facts = map[string]string{"binlog": "unknown"}
		return n
	}
	n.Health, n.Facts = binlogHealth(*info.binlog)
	return n
}

// binlogHealth maps the binlog state to the node's health and facts: the
// oldest binlog the server still has, how long it keeps them, and the GTID
// mode. With no binlog file listed the connector has nothing to stream from:
// a warning. Whether the file the connector needs is already purged needs the
// connector's stored position, which is mysql_binlog_status's job (Q8c), not
// the map's. Server-provided names are capped: they are untrusted text shown
// to the model.
func binlogHealth(st BinlogStatus) (Health, map[string]string) {
	facts := map[string]string{"gtid_mode": "unknown"}
	if st.GTIDMode != "" {
		facts["gtid_mode"] = truncFact(st.GTIDMode)
	}
	switch {
	case st.RetentionHours != nil:
		facts["binlog_expire"] = HumanAge(time.Duration(*st.RetentionHours) * time.Hour)
	case st.ExpireSeconds > 0:
		facts["binlog_expire"] = HumanAge(time.Duration(st.ExpireSeconds) * time.Second)
	default:
		facts["binlog_expire"] = "never"
	}
	if len(st.Binlogs) == 0 {
		facts["binlog_oldest"] = "none"
		return HealthWarn, facts
	}
	facts["binlog_oldest"] = truncFact(st.Binlogs[0].Name)
	facts["binlog_files"] = strconv.Itoa(len(st.Binlogs) + st.BinlogsOmitted)
	return HealthOK, facts
}
