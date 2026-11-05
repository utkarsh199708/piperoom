package dbtools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// openTxnReader is implemented by SourceDB implementations that count every
// open transaction regardless of age and name the oldest one and the oldest
// writer (internal/clients/postgres). It is optional: a client without it
// (MySQL, test fakes) simply reports no oldest_open.
type openTxnReader interface {
	OpenTransactions(ctx context.Context, tables []string, identifyAll bool) (postgres.OpenTransactions, error)
}

// openTxn is one open transaction of any age as the tools report it. It has the
// allow-listed fields of otherWriter, plus the application name for a session
// on the pipeline's tables (or any session, unscoped) and has_xid. No user,
// database name, client address or query text.
type openTxn struct {
	PID int `json:"pid"`
	// ApplicationName is empty for a session outside the pipeline's tables in
	// a room: it belongs to another pipeline or team.
	ApplicationName string `json:"application_name,omitempty"`
	// OnPipelineTables is true when the session holds or waits for a lock on
	// one of the pipeline's source tables (always false unscoped).
	OnPipelineTables bool   `json:"on_pipeline_tables"`
	SameDatabase     bool   `json:"same_database"`
	State            string `json:"state"`
	Age              string `json:"age"`
	AgeSeconds       int64  `json:"age_seconds"`
	// HasXID marks a writer: it holds back every logical slot's restart_lsn
	// (the WAL horizon) until it ends, whatever its age.
	HasXID        bool   `json:"has_xid"`
	XIDAge        int64  `json:"xid_age,omitempty"`
	WaitEventType string `json:"wait_event_type,omitempty"`
}

// openTxnFields are the age-independent fields pg_long_transactions adds to
// Data (scoped and unscoped alike).
type openTxnFields struct {
	// OpenTransactions counts every open transaction of any age in every
	// database (not walsenders, not this tool's own reads); OpenWriters those
	// holding an xid. Unlike other_writers they are not filtered by age.
	OpenTransactions *int `json:"open_transactions,omitempty"`
	OpenWriters      *int `json:"open_writers,omitempty"`
	// OldestOpen is the oldest open transaction of any age, even when younger
	// than older_than_minutes; OldestOpenWriter is the oldest writer, set only
	// when OldestOpen itself holds no xid.
	OldestOpen       *openTxn `json:"oldest_open,omitempty"`
	OldestOpenWriter *openTxn `json:"oldest_open_writer,omitempty"`
	// PreparedTransactions counts two-phase transactions, which hold the WAL
	// horizon with no session.
	PreparedTransactions     int   `json:"prepared_transactions,omitempty"`
	OldestPreparedAgeSeconds int64 `json:"oldest_prepared_age_seconds,omitempty"`
	// OpenNote says why the fields above are missing (the read failed).
	OpenNote string `json:"open_note,omitempty"`
}

// toOpenTxn maps a client row onto the tool shape. State and wait type keep
// Postgres's fixed vocabularies (anything else becomes "?"), and the
// application name, client-chosen free text, is kept to one short line.
func toOpenTxn(t postgres.OpenTransaction) openTxn {
	state := t.State
	if !backendStates[state] {
		state = "?"
	}
	wait := t.WaitEventType
	if wait != "" && !waitEventTypes[wait] {
		wait = "?"
	}
	return openTxn{
		PID: t.PID, ApplicationName: cutShort(oneLine(t.ApplicationName), 64),
		OnPipelineTables: t.OnTables, SameDatabase: t.SameDatabase, State: state,
		Age: topology.HumanAge(time.Duration(t.AgeSeconds) * time.Second), AgeSeconds: t.AgeSeconds,
		HasXID: t.HasXID, XIDAge: t.XIDAge, WaitEventType: wait,
	}
}

// readOpenTxns fills the age-independent fields, or OpenNote when the read
// fails; ok is false when the client cannot do the read at all.
func (d Deps) readOpenTxns(ctx context.Context, tables []string, identifyAll bool) (f openTxnFields, ok bool) {
	r, ok := d.Src.(openTxnReader)
	if !ok {
		return openTxnFields{}, false
	}
	ot, err := r.OpenTransactions(ctx, tables, identifyAll)
	if err != nil {
		return openTxnFields{OpenNote: "oldest open transaction unavailable: " + cutShort(oneLine(err.Error()), 200)}, true
	}
	f.OpenTransactions, f.OpenWriters = &ot.Open, &ot.Writers
	f.PreparedTransactions, f.OldestPreparedAgeSeconds = ot.Prepared, ot.OldestPreparedAgeSeconds
	if ot.Oldest != nil {
		o := toOpenTxn(*ot.Oldest)
		f.OldestOpen = &o
	}
	if ot.OldestWriter != nil && (f.OldestOpen == nil || !f.OldestOpen.HasXID) {
		w := toOpenTxn(*ot.OldestWriter)
		f.OldestOpenWriter = &w
	}
	return f, true
}

// writeOpenTxnLines renders the age-independent block that leads a
// pg_long_transactions Summary, so truncation never cuts it. threshold is the
// call's older_than_minutes, to say plainly when the holder is younger.
func writeOpenTxnLines(sb *strings.Builder, f openTxnFields, threshold time.Duration) {
	if f.OpenNote != "" {
		sb.WriteString(f.OpenNote + "\n")
		return
	}
	if f.OpenTransactions == nil {
		return
	}
	open, writers := *f.OpenTransactions, *f.OpenWriters
	fmt.Fprintf(sb, "open transactions of any age (all databases, not filtered by older_than_minutes): %d, %d of them %s (has_xid)\n",
		open, writers, tools.Plural(writers, "is a writer", "are writers"))
	if f.OldestOpen != nil {
		sb.WriteString("oldest open transaction: ")
		writeOpenTxn(sb, *f.OldestOpen)
		writeHolderClause(sb, *f.OldestOpen, threshold)
	}
	if f.OldestOpenWriter != nil {
		sb.WriteString("oldest open writer: ")
		writeOpenTxn(sb, *f.OldestOpenWriter)
		writeHolderClause(sb, *f.OldestOpenWriter, threshold)
	}
	if writers == 0 && f.PreparedTransactions == 0 {
		sb.WriteString("no open transaction holds an xid, so none holds the WAL horizon of a replication slot\n")
	}
	if f.PreparedTransactions > 0 {
		fmt.Fprintf(sb, "%d prepared (two-phase) %s, the oldest %s old: %s the WAL horizon with no session (pg_prepared_xacts)\n",
			f.PreparedTransactions, tools.Plural(f.PreparedTransactions, "transaction", "transactions"),
			topology.HumanAge(time.Duration(f.OldestPreparedAgeSeconds)*time.Second),
			tools.Plural(f.PreparedTransactions, "it holds", "they hold"))
	}
}

// writeOpenTxn renders one open transaction on the current line.
func writeOpenTxn(sb *strings.Builder, t openTxn) {
	fmt.Fprintf(sb, "pid=%d", t.PID)
	if t.ApplicationName != "" {
		fmt.Fprintf(sb, " app=%s", t.ApplicationName)
	}
	// HumanAge rounds down to minutes; a holder's exact age matters when it
	// is compared with a threshold, so the seconds follow.
	age := t.Age
	if t.AgeSeconds >= 60 {
		age = fmt.Sprintf("%s (%ds)", t.Age, t.AgeSeconds)
	}
	fmt.Fprintf(sb, " on_pipeline_tables=%s same_database=%s state=%s age=%s has_xid=%s wait_type=%s",
		yesNo(t.OnPipelineTables), yesNo(t.SameDatabase), orDash(t.State), age, yesNo(t.HasXID), orDash(t.WaitEventType))
}

// writeHolderClause ends the line with what the transaction means for the
// replication slots.
func writeHolderClause(sb *strings.Builder, t openTxn, threshold time.Duration) {
	if !t.HasXID {
		sb.WriteString(" (read-only: it holds no xid, so it does not hold the WAL horizon)\n")
		return
	}
	sb.WriteString(" (it holds an xid: it holds back every logical slot's restart_lsn, the WAL horizon, until it ends")
	if time.Duration(t.AgeSeconds)*time.Second < threshold {
		fmt.Fprintf(sb, "; it is younger than the %s threshold, so it is not in the long-transaction list below", topology.HumanAge(threshold))
	}
	sb.WriteString(")\n")
}

// horizonHolderMinAge is how old an open writer (or prepared transaction)
// must be to count as what holds a flat restart_lsn. restart_lsn only moves
// when the decoder reaches a running-xacts record (logged about every 15 s on
// a busy server) and the client confirms past it, so on a healthy slot it can
// stay flat for a few seconds while confirmed_flush_lsn advances. A writer
// open for twice that interval was in progress at a running-xacts record and
// is what pins the slot, not the cadence.
const horizonHolderMinAge = 30 * time.Second

// horizonHolder finds what holds a flat restart_lsn: the oldest open writer,
// else the oldest prepared transaction, at least horizonHolderMinAge old.
// checked is false when the client cannot tell (no openTxnReader) or the read
// failed; note says why.
func (d Deps) horizonHolder(ctx context.Context, tables []string, identifyAll bool) (holder *openTxn, prepared int, preparedAge int64, checked bool, note string) {
	f, ok := d.readOpenTxns(ctx, tables, identifyAll)
	if !ok {
		return nil, 0, 0, false, "the source client cannot list open transactions, so what holds restart_lsn was not checked"
	}
	if f.OpenNote != "" {
		return nil, 0, 0, false, f.OpenNote
	}
	w := f.OldestOpenWriter
	if f.OldestOpen != nil && f.OldestOpen.HasXID {
		w = f.OldestOpen
	}
	if w != nil && time.Duration(w.AgeSeconds)*time.Second >= horizonHolderMinAge {
		holder = w
	}
	if f.PreparedTransactions > 0 && time.Duration(f.OldestPreparedAgeSeconds)*time.Second >= horizonHolderMinAge {
		prepared, preparedAge = f.PreparedTransactions, f.OldestPreparedAgeSeconds
	}
	return holder, prepared, preparedAge, true, ""
}
