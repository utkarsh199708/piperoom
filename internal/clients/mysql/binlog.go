package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// maxGTIDBytes caps each GTID set kept in BinlogStatus. A server's set after
// many failovers holds a few hundred source ids, far below this; a set past
// it is refused rather than cut, because a cut set is a different set.
const maxGTIDBytes = 1 << 20

const needBinlog = "REPLICATION CLIENT"

// BinlogStatus reads the binlog state of the server (design §6.7): GTID mode
// and sets, server id and UUID, expiry, the current write position and the
// binlog files the server still has, oldest first. Positions and names only,
// never event contents. On RDS/Aurora RetentionHours comes from
// mysql.rds_show_configuration (best effort).
func (d *DB) BinlogStatus(ctx context.Context) (BinlogStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	var st BinlogStatus
	if err := d.queryRow(ctx, `
		SELECT @@global.gtid_mode, @@global.gtid_executed, @@global.gtid_purged,
		       @@server_id, @@server_uuid, @@global.binlog_expire_logs_seconds`).
		Scan(&st.GTIDMode, &st.GTIDExecuted, &st.GTIDPurged, &st.ServerID, &st.ServerUUID, &st.ExpireSeconds); err != nil {
		return BinlogStatus{}, fail("read binlog variables", "", err)
	}
	if len(st.GTIDExecuted) > maxGTIDBytes || len(st.GTIDPurged) > maxGTIDBytes {
		return BinlogStatus{}, fmt.Errorf("mysql: GTID set longer than %d bytes", maxGTIDBytes)
	}
	st.GTIDMode = cutText(st.GTIDMode, 32)
	st.ServerUUID = cutText(st.ServerUUID, 64)

	if err := d.currentPosition(ctx, &st); err != nil {
		return BinlogStatus{}, err
	}
	if err := d.binlogFiles(ctx, &st); err != nil {
		return BinlogStatus{}, err
	}
	if h, ok := d.rdsRetentionHours(ctx); ok {
		st.RetentionHours = h
	}
	return st, nil
}

// statusStatements returns the two forms of the current-position statement,
// the one this server version takes first: SHOW MASTER STATUS was renamed in
// 8.2 and removed in 8.4.
func (d *DB) statusStatements() [2]string {
	const binary, master = "SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"
	if d.major > 8 || (d.major == 8 && d.minor >= 2) {
		return [2]string{binary, master}
	}
	return [2]string{master, binary}
}

// currentPosition reads File and Position. An "unknown statement" error
// falls back once to the other form; an empty result is the binlog off.
func (d *DB) currentPosition(ctx context.Context, st *BinlogStatus) error {
	stmts := d.statusStatements()
	rows, err := d.query(ctx, stmts[0])
	if err != nil && errNumber(err) == errParse {
		rows, err = d.query(ctx, stmts[1])
	}
	if err != nil {
		return fail("read binary log status", needBinlog, err)
	}
	defer rows.Close()
	recs, err := scanNamed(rows, 1)
	if err != nil {
		return fail("read binary log status", needBinlog, err)
	}
	if len(recs) == 0 {
		return nil
	}
	st.File = cutText(recs[0]["file"], 255)
	if p := recs[0]["position"]; p != "" {
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return fmt.Errorf("mysql: binary log position %q is not a number", cutText(p, 30))
		}
		st.Pos = n
	}
	return nil
}

// binlogFiles reads SHOW BINARY LOGS, oldest first, and keeps the oldest
// MaxBinlogFiles. Error 1381 (binary logging off) is an empty list.
func (d *DB) binlogFiles(ctx context.Context, st *BinlogStatus) error {
	rows, err := d.query(ctx, `SHOW BINARY LOGS`)
	if err != nil {
		if errNumber(err) == errNoBinaryLogging {
			return nil
		}
		return fail("list binary logs", needBinlog, err)
	}
	defer rows.Close()
	recs, err := scanNamed(rows, -1)
	if err != nil {
		return fail("list binary logs", needBinlog, err)
	}
	for _, r := range recs {
		if len(st.Binlogs) == MaxBinlogFiles {
			st.BinlogsOmitted++
			continue
		}
		size, err := strconv.ParseInt(r["file_size"], 10, 64)
		if err != nil {
			return fmt.Errorf("mysql: binary log size %q is not a number", cutText(r["file_size"], 30))
		}
		st.Binlogs = append(st.Binlogs, BinlogFile{Name: cutText(r["log_name"], 255), Size: size})
	}
	return nil
}

// scanNamed reads rows of a SHOW statement into maps keyed by lower-case
// column name; limit < 0 reads them all. The column sets differ between
// versions (Encrypted since 8.0.14), so columns are taken by name.
func scanNamed(rows *sql.Rows, limit int) ([]map[string]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]string
	for rows.Next() {
		if limit >= 0 && len(out) == limit {
			continue // drain
		}
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]string, len(cols))
		for i, c := range cols {
			m[strings.ToLower(c)] = vals[i].String
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
