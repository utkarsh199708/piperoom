package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql/gtid"
)

// MySQL positions in connect_reset_source_offsets (Q8e,
// docs/design/mysql-source.md §8.2). A Debezium MySQL connector stores its
// binlog position as {"server": topic.prefix} -> {file, pos, gtids, ...}. The
// stored-shape check of connect_stop_reset.go only knows "same keys, same
// types"; this file adds what only the server can say: whether the binlog the
// position names still exists, whether the position is past the end, and
// whether its GTID set still covers what the server purged. A position that
// fails any of these makes the connector fail on start (or silently skip
// transactions), so it is refused at Validate and again at Execute.
//
// The binlog state comes from clients.BinlogReader (Q0 item 7), which the
// MySQL client (Q8b) implements.

var (
	// ErrPositionPurged refuses a MySQL position the server no longer has: the
	// binlog file it names is purged, or its GTID set misses transactions in
	// gtid_purged. The connector would fail on start, or skip them.
	ErrPositionPurged = errors.New("actions: the source no longer has the transactions this position needs (purged)")
	// ErrPositionPastEnd refuses a MySQL position beyond what the server has
	// written: past the end of its binlog file or a GTID set the server has
	// not executed.
	ErrPositionPastEnd = errors.New("actions: the position is past the end of what the source has written")
)

// mysqlConnectorSuffix ends the class of a Debezium MySQL source connector
// (io.debezium.connector.mysql.MySqlConnector).
const mysqlConnectorSuffix = "MySqlConnector"

// isMySQLConnector reports whether cfg is a Debezium MySQL source connector.
func isMySQLConnector(cfg map[string]string) bool {
	class := strings.TrimSpace(cfg["connector.class"])
	return strings.HasPrefix(class, "io.debezium.connector.") && strings.HasSuffix(class, mysqlConnectorSuffix)
}

// binlogReaderOf returns the binlog reader of a lifecycle action: the one
// wiring passed, else the source client when it has one (a MySQL room's
// source client is the MySQL client, which implements it).
func binlogReaderOf(explicit clients.BinlogReader, src any) (clients.BinlogReader, bool) {
	if explicit != nil {
		return explicit, true
	}
	br, ok := src.(clients.BinlogReader)
	return br, ok
}

// readBinlog reads the MySQL server's binlog state for a position check.
func readBinlog(ctx context.Context, br clients.BinlogReader, ok bool, connector string) (clients.BinlogStatus, error) {
	if !ok || br == nil {
		return clients.BinlogStatus{}, fmt.Errorf("actions: %s: connector %s is a MySQL connector and no binlog reader of its source is configured, so Piperoom cannot check a MySQL position: %w",
			ResetSourceOffsetsName, connector, clients.ErrNotSupported)
	}
	st, err := br.BinlogStatus(ctx)
	if err != nil {
		return clients.BinlogStatus{}, fmt.Errorf("actions: %s: read the source's binlog status to check the position of %s: %w", ResetSourceOffsetsName, connector, err)
	}
	return st, nil
}

// mysqlOffsetKey is how one key of a MySQL offset is checked.
type mysqlOffsetKey byte

const (
	mysqlKeyString mysqlOffsetKey = 's'
	mysqlKeyNumber mysqlOffsetKey = 'n'
	mysqlKeyAny    mysqlOffsetKey = 'a' // a scalar of the stored type (checked by the shape check)
)

// mysqlOffsetKeys are the keys a Debezium MySQL offset may carry. file, pos
// and gtids are the position; row and event locate the event inside a
// multi-row binlog event; ts_sec and server_id say when and on which server;
// transaction_id and the transaction_data_collection_order_ prefix are the
// transaction-metadata context (provide.transaction.metadata). "snapshot"
// (and any other key) is refused: a mid-snapshot position is never written.
// Confirm against the Debezium MySQL docs (design §8.2 "confirm" note).
var mysqlOffsetKeys = map[string]mysqlOffsetKey{
	"file":           mysqlKeyString,
	"pos":            mysqlKeyNumber,
	"gtids":          mysqlKeyString,
	"row":            mysqlKeyNumber,
	"event":          mysqlKeyNumber,
	"ts_sec":         mysqlKeyNumber,
	"server_id":      mysqlKeyNumber,
	"transaction_id": mysqlKeyAny,
}

const mysqlTxnOrderPrefix = "transaction_data_collection_order_"

// mysqlPosition is a MySQL offset's position fields.
type mysqlPosition struct {
	file    string
	pos     int64
	hasFile bool
	gtids   string
	hasGTID bool
	// serverID is the offset's server_id (the server that wrote the event),
	// hasServerID whether the offset carries one.
	serverID    int64
	hasServerID bool
}

// mysqlMinBinlogPos is the first event offset of a binlog file: bytes 0 to 3
// are the file's magic number, so a pos below it names no event.
const mysqlMinBinlogPos = 4

// parseMySQLOffset checks the keys and types of a MySQL offset without I/O.
func parseMySQLOffset(connector string, off map[string]any) (mysqlPosition, error) {
	var mp mysqlPosition
	refuse := func(format string, args ...any) (mysqlPosition, error) {
		return mysqlPosition{}, fmt.Errorf("actions: %s: connector %s: %s: %w", ResetSourceOffsetsName, connector, fmt.Sprintf(format, args...), ErrSourcePosition)
	}
	keys := make([]string, 0, len(off))
	for k := range off {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := off[k]
		if k == "snapshot" {
			return refuse("the offset carries \"snapshot\": a mid-snapshot position is never written; give a streaming position (file, pos, gtids)")
		}
		kind, known := mysqlOffsetKeys[k]
		if !known && strings.HasPrefix(k, mysqlTxnOrderPrefix) {
			kind, known = mysqlKeyNumber, true
		}
		if !known {
			return refuse("%q is not a key of a Debezium MySQL binlog position (file, pos, gtids, row, event, ts_sec, server_id)", shortText(k))
		}
		switch kind {
		case mysqlKeyString:
			s, ok := v.(string)
			if !ok || s == "" {
				return refuse("%s must be a non-empty string", k)
			}
		case mysqlKeyNumber:
			n, ok := v.(json.Number)
			if !ok {
				return refuse("%s must be a number", k)
			}
			if x, err := strconv.ParseInt(n.String(), 10, 64); err != nil || x < 0 {
				return refuse("%s must be a whole non-negative number", k)
			}
		}
	}
	if s, ok := off["file"].(string); ok {
		mp.file, mp.hasFile = s, true
	}
	if n, ok := off["pos"].(json.Number); ok {
		mp.pos, _ = strconv.ParseInt(n.String(), 10, 64) // checked above
	}
	if mp.hasFile != (off["pos"] != nil) {
		return refuse("file and pos go together: give both or neither")
	}
	if mp.hasFile && mp.pos < mysqlMinBinlogPos {
		return refuse("pos %d names no event: binlog events start at offset %d (the first 4 bytes are the file header)", mp.pos, mysqlMinBinlogPos)
	}
	if n, ok := off["server_id"].(json.Number); ok {
		mp.serverID, _ = strconv.ParseInt(n.String(), 10, 64) // checked above
		mp.hasServerID = true
	}
	if s, ok := off["gtids"].(string); ok {
		mp.gtids, mp.hasGTID = s, true
	}
	if !mp.hasFile && !mp.hasGTID {
		return refuse("a MySQL position needs file and pos, or gtids")
	}
	return mp, nil
}

// binlogName splits "mysql-bin.000042" into its base and sequence number.
func binlogName(name string) (base string, seq int64, ok bool) {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(name[i+1:], 10, 64)
	if err != nil || n < 0 {
		return "", 0, false
	}
	return name[:i], n, true
}

// checkMySQLPosition checks one position against the connector's config and
// the server's binlog state, without I/O: the partition is {"server":
// topic.prefix}; with gtid_mode ON the offset carries gtids, without GTIDs it
// carries file and pos; the file is one the server still has and pos is not
// past its end; the GTID set holds gtid_purged and is within gtid_executed.
func checkMySQLPosition(connector string, cfg map[string]string, pos sourcePosition, st clients.BinlogStatus) error {
	refuse := func(sentinel error, format string, args ...any) error {
		if sentinel != nil {
			return fmt.Errorf("actions: %s: connector %s: %s: %w: %w", ResetSourceOffsetsName, connector, fmt.Sprintf(format, args...), ErrSourcePosition, sentinel)
		}
		return fmt.Errorf("actions: %s: connector %s: %s: %w", ResetSourceOffsetsName, connector, fmt.Sprintf(format, args...), ErrSourcePosition)
	}
	prefix := strings.TrimSpace(cfg["topic.prefix"])
	if prefix == "" {
		return refuse(nil, "it has no topic.prefix, so its MySQL partition cannot be checked")
	}
	if len(pos.Partition) != 1 || !sameJSON(pos.Partition["server"], prefix) {
		return refuse(nil, "a MySQL partition is {\"server\": %q}, not %s", prefix, jsonText(pos.Partition))
	}
	mp, err := parseMySQLOffset(connector, pos.Offset)
	if err != nil {
		return err
	}
	gtidsOn := strings.EqualFold(strings.TrimSpace(st.GTIDMode), "ON")
	if gtidsOn && !mp.hasGTID {
		return refuse(nil, "the server runs with gtid_mode=ON, so the position must carry gtids (the connector resumes by GTID set)")
	}
	if !gtidsOn && !mp.hasFile {
		return refuse(nil, "the server runs with gtid_mode=%s, so the position must carry file and pos", orUnknown(st.GTIDMode))
	}
	// Without GTIDs a file/pos position means something only on the server
	// that wrote it: after a failover another server's mysql-bin.0000NN can
	// fall inside this server's kept range and pass the file checks, then
	// resume at an arbitrary event (design §7.2's other_server case).
	if !gtidsOn && mp.hasServerID && st.ServerID != 0 && mp.serverID != st.ServerID {
		return refuse(nil, "the position's server_id %d is not the source's @@server_id %d, and without GTIDs a binlog file and pos mean something only on the server that wrote them: a position of another server (a failover?)", mp.serverID, st.ServerID)
	}
	if mp.hasFile {
		if err := checkBinlogFile(mp, st, refuse); err != nil {
			return err
		}
	}
	if mp.hasGTID {
		if strings.TrimSpace(st.GTIDExecuted) == "" {
			return refuse(nil, "the position carries gtids but the server reports no gtid_executed (GTIDs off)")
		}
		target, err := gtid.Parse(mp.gtids)
		if err != nil {
			return refuse(nil, "gtids is not a GTID set: %v", err)
		}
		executed, err := gtid.Parse(st.GTIDExecuted)
		if err != nil {
			return fmt.Errorf("actions: %s: read the server's gtid_executed: %w", ResetSourceOffsetsName, err)
		}
		purged, err := gtid.Parse(st.GTIDPurged)
		if err != nil {
			return fmt.Errorf("actions: %s: read the server's gtid_purged: %w", ResetSourceOffsetsName, err)
		}
		if missing := purged.Subtract(target); !missing.IsEmpty() {
			return refuse(ErrPositionPurged, "the GTID set misses %d transactions of gtid_purged: the server purged them, so the connector could never read them", missing.Count())
		}
		if extra := target.Subtract(executed); !extra.IsEmpty() {
			return refuse(ErrPositionPastEnd, "the GTID set holds %d transactions the server has not executed (not within gtid_executed): past the end", extra.Count())
		}
	}
	return nil
}

// checkBinlogFile checks file and pos against the server's binlog list.
func checkBinlogFile(mp mysqlPosition, st clients.BinlogStatus, refuse func(error, string, ...any) error) error {
	if st.File == "" {
		return refuse(nil, "the server reports no current binlog file (binary logging off?), so a file position cannot be checked")
	}
	if mp.file == st.File {
		if mp.pos > st.Pos {
			return refuse(ErrPositionPastEnd, "pos %d is past the current write position %s:%d", mp.pos, st.File, st.Pos)
		}
		return nil
	}
	for _, f := range st.Binlogs {
		if f.Name == mp.file {
			if mp.pos > f.Size {
				return refuse(ErrPositionPastEnd, "pos %d is past the end of %s (%d bytes)", mp.pos, f.Name, f.Size)
			}
			return nil
		}
	}
	base, seq, ok := binlogName(mp.file)
	curBase, curSeq, curOK := binlogName(st.File)
	if !ok || !curOK || base != curBase {
		return refuse(nil, "binlog %s never existed on this server (its binlogs are %s.*): a position of another server?", shortText(mp.file), curBase)
	}
	if seq > curSeq {
		return refuse(ErrPositionPastEnd, "binlog %s is after the current one, %s: past the end", mp.file, st.File)
	}
	if len(st.Binlogs) > 0 {
		if _, oldest, ok := binlogName(st.Binlogs[0].Name); ok && seq < oldest {
			return refuse(ErrPositionPurged, "the binlog the position names is purged: %s is older than the oldest the server still has, %s", mp.file, st.Binlogs[0].Name)
		}
		if st.BinlogsOmitted > 0 {
			if _, newestKept, ok := binlogName(st.Binlogs[len(st.Binlogs)-1].Name); ok && seq > newestKept {
				// Among the newest files the listing left out (MaxBinlogFiles):
				// it exists, but its size is unknown here.
				return nil
			}
		}
	}
	return refuse(nil, "binlog %s is not among the files the server has (SHOW BINARY LOGS): it never existed", mp.file)
}

// mysqlPositionNote is the Preview line of a MySQL target position: the
// server's current position and, with GTIDs, how many transactions the
// target is behind the server and how it differs from the stored position.
func mysqlPositionNote(st clients.BinlogStatus, target sourcePosition, stored map[string]any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "MySQL source: current binlog position %s:%d", shortText(st.File), st.Pos)
	if len(st.Binlogs) > 0 {
		fmt.Fprintf(&b, ", oldest binlog kept %s", shortText(st.Binlogs[0].Name))
	}
	fmt.Fprintf(&b, ", gtid_mode %s", orUnknown(cutText(st.GTIDMode, 32)))
	ts, _ := target.Offset["gtids"].(string)
	t, err := gtid.Parse(ts)
	if ts == "" || err != nil {
		return b.String()
	}
	if executed, err := gtid.Parse(st.GTIDExecuted); err == nil {
		fmt.Fprintf(&b, "; the target is %d transactions behind gtid_executed (the connector reads them after it resumes)", executed.Subtract(t).Count())
	}
	if ss, _ := stored["gtids"].(string); ss != "" {
		if s, err := gtid.Parse(ss); err == nil {
			fmt.Fprintf(&b, "; versus the stored position it re-reads %d and skips %d transactions", s.Subtract(t).Count(), t.Subtract(s).Count())
		}
	}
	return b.String()
}

// mysqlClearNote is what a full reset means for a MySQL connector whose
// snapshot.mode does not snapshot data: it starts at the server's current
// binlog position, so every change between the stored position and now is
// skipped (design §8.2).
func mysqlClearNote(cfg map[string]string) string {
	switch strings.ToLower(strings.TrimSpace(cfg["snapshot.mode"])) {
	case "no_data", "schema_only", "never", "recovery", "schema_only_recovery":
		return "; on MySQL the connector then starts at the server's current binlog position and SKIPS every change written between the stored position and now"
	case "", "initial", "when_needed", "always":
		return "; on MySQL the snapshot re-reads the tables, then streaming starts at the binlog position the snapshot recorded"
	}
	return ""
}
