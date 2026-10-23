package mysqltools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/mysql/gtid"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// mysqlConnectorClass is the Debezium MySQL source connector's class; a
// pipeline connector of another class is listed but not compared.
const mysqlConnectorClass = "io.debezium.connector.mysql.MySqlConnector"

// Caps on what reaches the summary. GTID sets stay whole in Data (the client
// already refuses one over 1 MiB); the summary prints at most
// maxGTIDSummaryBytes of each (design §6.7).
const (
	maxGTIDSummaryBytes = 200
	maxNoteBytes        = 200
	maxConnectors       = 50
)

// Verdict severities, most severe first.
const (
	SeverityFault   = "fault"
	SeverityWarning = "warning"
	SeverityNote    = "note"
)

// Verdict codes (stable; scenarios and evals match on them).
const (
	CodeBinlogPurged   = "binlog_purged"
	CodeOtherServer    = "other_server"
	CodePastEnd        = "past_end"
	CodeRetentionShort = "retention_short"
	CodeServerIDClash  = "server_id_clash"
	CodeGTIDOff        = "gtid_off"
	CodeBinlogOff      = "binlog_off"
)

// BinlogStatusData is the tool's Data.
type BinlogStatusData struct {
	PipelineID string          `json:"pipeline_id,omitempty"`
	Server     ServerData      `json:"server"`
	Connectors []ConnectorData `json:"connectors"`
	// Verdicts are sorted most severe first.
	Verdicts []Verdict `json:"verdicts"`
	Notes    []string  `json:"notes,omitempty"`
}

// ServerData is the source server's binlog state as BinlogStatus read it.
type ServerData struct {
	File           string             `json:"file"`
	Pos            int64              `json:"pos"`
	GTIDMode       string             `json:"gtid_mode"`
	GTIDExecuted   string             `json:"gtid_executed"`
	GTIDPurged     string             `json:"gtid_purged"`
	ServerID       int64              `json:"server_id"`
	ServerUUID     string             `json:"server_uuid"`
	Binlogs        []mysql.BinlogFile `json:"binlogs"`
	BinlogsOmitted int                `json:"binlogs_omitted,omitempty"`
	ExpireSeconds  int64              `json:"expire_seconds"`
	RetentionHours *int64             `json:"retention_hours,omitempty"`
}

// ConnectorData is one pipeline connector and its stored position.
type ConnectorData struct {
	Name  string `json:"name"`
	State string `json:"state,omitempty"`
	// DatabaseServerID is the connector's database.server.id; "" when the
	// config could not be read or has none.
	DatabaseServerID string          `json:"database_server_id,omitempty"`
	Stored           *StoredPosition `json:"stored,omitempty"`
	Notes            []string        `json:"notes,omitempty"`
}

// StoredPosition is the Debezium MySQL offset of a connector: positions only.
type StoredPosition struct {
	File     string `json:"file,omitempty"`
	Pos      int64  `json:"pos,omitempty"`
	GTIDs    string `json:"gtids,omitempty"`
	ServerID int64  `json:"server_id,omitempty"`
	// TsSec is the offset's ts_sec: the time of the last event the
	// connector recorded; 0 when absent.
	TsSec    int64 `json:"ts_sec,omitempty"`
	Snapshot bool  `json:"snapshot,omitempty"`
}

// Verdict is one finding, about one connector or ("" Connector) the server.
type Verdict struct {
	Severity  string `json:"severity"`
	Code      string `json:"code"`
	Connector string `json:"connector,omitempty"`
	Text      string `json:"text"`
}

// binlogStatusTool is mysql_binlog_status (design §7.2). Read-only: it calls
// BinlogStatus, and on Connect only Config, Status and SourceOffsets.
type binlogStatusTool struct{ d Deps }

func (t *binlogStatusTool) Name() string     { return NameBinlogStatus }
func (t *binlogStatusTool) Kind() tools.Kind { return tools.KindRead }
func (t *binlogStatusTool) InputSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "optional: one of this room's MySQL source connectors; default: all of them"},
		},
		"additionalProperties": false,
		"required":             []string{},
	}
}
func (t *binlogStatusTool) Description() string {
	return "MySQL pipelines only. Reads the source server's binlog state (current file and position, the binlog files it still keeps, " +
		"binlog_expire_logs_seconds or the RDS retention hours, gtid_mode, gtid_executed, gtid_purged, server_id) and compares it with each " +
		"Debezium MySQL connector's stored offset (file, pos, gtids, server_id, ts_sec) and its database.server.id. " +
		"It says when the binlog the connector needs is purged, when the stored position is from another server or past the end, " +
		"when retention is shorter than a stopped connector's age, and when database.server.id clashes. Positions only, never event contents."
}

func (t *binlogStatusTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var in struct {
		Connector string `json:"connector"`
	}
	if len(input) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(input)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
		}
	}
	sc, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return tools.Result{}, err
	}
	var p owners.Pipeline
	if !sc.Unscoped() {
		p = sc.Pipeline()
		if k := sc.SourceKind(); k != owners.SourceKindMySQL {
			if k == "" {
				k = "none (not a cdc pipeline)"
			}
			return tools.Result{}, fmt.Errorf("%w: %s reads MySQL sources only; pipeline %q has source_kind %s", tools.ErrBadInput, NameBinlogStatus, p.ID, k)
		}
	}
	targets, err := connectorTargets(sc, in.Connector)
	if err != nil {
		return tools.Result{}, err
	}
	br, ok := t.d.Src.(mysql.BinlogReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("mysqltools: %s: %w: the source client cannot read the binlog state (not a MySQL client)", NameBinlogStatus, clients.ErrNotSupported)
	}

	ctx, cancel := withTimeout(ctx)
	defer cancel()
	st, err := br.BinlogStatus(ctx)
	if err != nil {
		return tools.Result{}, fmt.Errorf("mysqltools: %s: %w", NameBinlogStatus, err)
	}

	data := BinlogStatusData{PipelineID: p.ID, Server: serverData(st)}
	data.Connectors, data.Notes = t.readConnectors(ctx, targets)
	var peers map[string]string
	if in.Connector != "" && !sc.Unscoped() {
		peers = t.peerServerIDs(ctx, p.Connectors, in.Connector)
	}
	data.Verdicts = verdicts(st, data.Connectors, peers, t.d.now())
	return result(data), nil
}

// connectorTargets resolves the connectors to read: the one named (after the
// scope check) or every pipeline connector, sorted and deduplicated.
// Unscoped, a connector is required: there is no pipeline to list.
func connectorTargets(sc tools.Scope, named string) ([]string, error) {
	if named != "" {
		if err := sc.CheckConnector(named); err != nil {
			return nil, err
		}
		return []string{named}, nil
	}
	if sc.Unscoped() {
		return nil, fmt.Errorf("%w: connector is required when the call has no room pipeline", tools.ErrBadInput)
	}
	names := append([]string(nil), sc.Pipeline().Connectors...)
	sort.Strings(names)
	out := names[:0]
	for i, n := range names {
		if i == 0 || n != names[i-1] {
			out = append(out, n)
		}
	}
	if len(out) > maxConnectors {
		return nil, fmt.Errorf("%w: pipeline has %d connectors, at most %d per call: name one", tools.ErrBadInput, len(out), maxConnectors)
	}
	return out, nil
}

// readConnectors reads each connector's config, status and stored offset.
// A read that fails becomes a note on that connector: the server's state is
// still worth reporting.
func (t *binlogStatusTool) readConnectors(ctx context.Context, names []string) ([]ConnectorData, []string) {
	out := make([]ConnectorData, 0, len(names))
	if t.d.Connect == nil {
		for _, n := range names {
			out = append(out, ConnectorData{Name: n})
		}
		return out, []string{"Kafka Connect is not configured: only the server's binlog state was read, no connector was compared"}
	}
	var notes []string
	offs, offsOK := t.d.Connect.(connect.SourceOffsetReader)
	if !offsOK {
		notes = append(notes, "this Connect client cannot read stored offsets: the connectors' positions were not compared")
	}
	offsUnsupported := false
	for _, n := range names {
		c := ConnectorData{Name: n}
		cfg, err := t.d.Connect.Config(ctx, n)
		switch {
		case err != nil:
			c.Notes = append(c.Notes, "config not read: "+errText(err))
		default:
			if class := cfg["connector.class"]; class != mysqlConnectorClass {
				c.Notes = append(c.Notes, "not a Debezium MySQL source connector (connector.class "+oneLine(class, 120)+"): not compared")
				out = append(out, c)
				continue
			}
			c.DatabaseServerID = oneLine(strings.TrimSpace(cfg["database.server.id"]), 32)
		}
		if s, err := t.d.Connect.Status(ctx, n); err != nil {
			c.Notes = append(c.Notes, "status not read: "+errText(err))
		} else {
			c.State = oneLine(s.State, 32)
		}
		if offsOK && !offsUnsupported {
			o, err := offs.SourceOffsets(ctx, n)
			switch {
			case errors.Is(err, clients.ErrNotSupported):
				offsUnsupported = true
				notes = append(notes, "this Connect has no offsets endpoint (KIP-875, Kafka Connect 3.5+): the connectors' positions were not compared")
			case err != nil:
				c.Notes = append(c.Notes, "stored offsets not read: "+errText(err))
			default:
				pos, extra, perr := storedPosition(o)
				if perr != nil {
					c.Notes = append(c.Notes, perr.Error())
				}
				if extra > 0 {
					c.Notes = append(c.Notes, fmt.Sprintf("%d further stored partitions not compared (a Debezium MySQL connector stores one)", extra))
				}
				c.Stored = pos
				if pos == nil && perr == nil {
					c.Notes = append(c.Notes, "no stored offset: the connector has not recorded a position yet (it snapshots on start)")
				}
			}
		}
		out = append(out, c)
	}
	return out, notes
}

// peerServerIDs reads database.server.id of the pipeline's other MySQL
// connectors, so a call naming one connector still sees a clash with them.
// A config that cannot be read is skipped: the clash check is best effort.
func (t *binlogStatusTool) peerServerIDs(ctx context.Context, all []string, named string) map[string]string {
	if t.d.Connect == nil {
		return nil
	}
	out := map[string]string{}
	for _, n := range all {
		if n == named || len(out) >= maxConnectors {
			continue
		}
		cfg, err := t.d.Connect.Config(ctx, n)
		if err != nil || cfg["connector.class"] != mysqlConnectorClass {
			continue
		}
		if id := oneLine(strings.TrimSpace(cfg["database.server.id"]), 32); id != "" {
			out[n] = id
		}
	}
	return out
}

// storedPosition picks the connector's stored binlog position: the first
// partition (in a stable order) whose offset holds a file or a GTID set.
// extra counts the other partitions holding one.
func storedPosition(offs []connect.SourceOffset) (*StoredPosition, int, error) {
	var found []connect.SourceOffset
	for _, o := range offs {
		if o.Offset == nil {
			continue
		}
		if _, ok := o.Offset["file"]; ok {
			found = append(found, o)
		} else if _, ok := o.Offset["gtids"]; ok {
			found = append(found, o)
		}
	}
	if len(found) == 0 {
		return nil, 0, nil
	}
	sort.SliceStable(found, func(i, j int) bool {
		return fmt.Sprint(found[i].Partition["server"]) < fmt.Sprint(found[j].Partition["server"])
	})
	m := found[0].Offset
	p := &StoredPosition{}
	var errs []string
	if v, ok := m["file"]; ok {
		f, _ := v.(string)
		if !binlogNameRe.MatchString(f) {
			errs = append(errs, "stored file is not a binlog file name")
		} else {
			p.File = f
		}
	}
	var err error
	if p.Pos, err = intValue(m, "pos"); err != nil {
		errs = append(errs, err.Error())
	}
	if p.ServerID, err = intValue(m, "server_id"); err != nil {
		errs = append(errs, err.Error())
	}
	if p.TsSec, err = intValue(m, "ts_sec"); err != nil {
		errs = append(errs, err.Error())
	}
	if v, ok := m["gtids"]; ok && v != nil {
		s, _ := v.(string)
		set, err := gtid.Parse(s)
		if err != nil {
			errs = append(errs, "stored gtids is not a GTID set")
		} else {
			p.GTIDs = set.String()
		}
	}
	switch v := m["snapshot"].(type) {
	case bool:
		p.Snapshot = v
	case string:
		p.Snapshot = v == "true" || v == "last"
	}
	if len(errs) > 0 {
		return p, len(found) - 1, fmt.Errorf("stored offset: %s", strings.Join(errs, "; "))
	}
	return p, len(found) - 1, nil
}

// intValue reads an integer offset value: Connect decodes numbers as
// json.Number; a fake or an older client may give float64 or a string.
func intValue(m map[string]any, k string) (int64, error) {
	v, ok := m[k]
	if !ok || v == nil {
		return 0, nil
	}
	var s string
	switch x := v.(type) {
	case json.Number:
		s = x.String()
	case string:
		s = x
	case float64:
		if x != float64(int64(x)) {
			return 0, fmt.Errorf("stored %s is not an integer", k)
		}
		return int64(x), nil
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("stored %s is not a number", k)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("stored %s is not an integer", k)
	}
	return n, nil
}

// binlogNameRe is a binlog file name: base name, a dot, a numeric suffix.
// Names come from the server or Connect (untrusted); anything else is not
// compared and never printed.
var binlogNameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,240}\.[0-9]{1,12}$`)

// splitBinlog splits "mysql-bin.000012" into its base name and number.
func splitBinlog(name string) (base string, n int64, ok bool) {
	if !binlogNameRe.MatchString(name) {
		return "", 0, false
	}
	i := strings.LastIndexByte(name, '.')
	n, err := strconv.ParseInt(name[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return name[:i], n, true
}

// serverData copies the read state into Data, keeping only names that look
// like binlog names (the rest are counted as omitted).
func serverData(st mysql.BinlogStatus) ServerData {
	sd := ServerData{
		File: safeName(st.File), Pos: st.Pos, GTIDMode: oneLine(st.GTIDMode, 32),
		GTIDExecuted: st.GTIDExecuted, GTIDPurged: st.GTIDPurged,
		ServerID: st.ServerID, ServerUUID: oneLine(st.ServerUUID, 64),
		BinlogsOmitted: st.BinlogsOmitted, ExpireSeconds: st.ExpireSeconds, RetentionHours: st.RetentionHours,
		Binlogs: []mysql.BinlogFile{},
	}
	for _, f := range st.Binlogs {
		if !binlogNameRe.MatchString(f.Name) {
			sd.BinlogsOmitted++
			continue
		}
		sd.Binlogs = append(sd.Binlogs, f)
	}
	return sd
}

func safeName(s string) string {
	if s == "" || binlogNameRe.MatchString(s) {
		return s
	}
	return "(not a binlog file name)"
}

// verdicts compares every connector's stored position with the server
// (design §7.2) and sorts the findings most severe first.
// peers are database.server.id values of pipeline connectors not in conns
// (read when the call named one connector).
func verdicts(st mysql.BinlogStatus, conns []ConnectorData, peers map[string]string, now time.Time) []Verdict {
	var out []Verdict
	add := func(sev, code, conn, text string) {
		out = append(out, Verdict{Severity: sev, Code: code, Connector: conn, Text: text})
	}
	binlogOn := st.File != "" || len(st.Binlogs) > 0
	if !binlogOn {
		add(SeverityFault, CodeBinlogOff, "", "the server has no binary log (log_bin is off, or SHOW BINARY LOGS returned nothing): a CDC connector cannot read it")
	}
	gtidOn := strings.EqualFold(st.GTIDMode, "ON")
	if !gtidOn {
		add(SeverityNote, CodeGTIDOff, "", "gtid_mode is "+orUnknown(oneLine(st.GTIDMode, 32))+", not ON: positions are file and offset, and a failover to a replica loses them")
	}
	executed, errExec := gtid.Parse(st.GTIDExecuted)
	purged, errPurged := gtid.Parse(st.GTIDPurged)
	curBase, curNum, curOK := splitBinlog(st.File)
	var oldestBase string
	var oldestNum int64
	oldestOK := false
	if len(st.Binlogs) > 0 {
		oldestBase, oldestNum, oldestOK = splitBinlog(st.Binlogs[0].Name)
	}

	serverIDs := map[string][]string{} // database.server.id -> connectors
	for _, c := range conns {
		if c.DatabaseServerID != "" {
			serverIDs[c.DatabaseServerID] = append(serverIDs[c.DatabaseServerID], c.Name)
		}
	}
	peerNames := make([]string, 0, len(peers))
	for n := range peers {
		peerNames = append(peerNames, n)
	}
	sort.Strings(peerNames)
	for _, n := range peerNames {
		serverIDs[peers[n]] = append(serverIDs[peers[n]], n)
	}

	for _, c := range conns {
		if c.DatabaseServerID != "" {
			if id, err := strconv.ParseInt(c.DatabaseServerID, 10, 64); err == nil && id == st.ServerID {
				add(SeverityFault, CodeServerIDClash, c.Name, fmt.Sprintf("database.server.id %d equals the source server's own server_id: the server refuses or drops the connector's binlog stream; pick an id no server or other client uses", id))
			}
			if others := serverIDs[c.DatabaseServerID]; len(others) > 1 {
				add(SeverityFault, CodeServerIDClash, c.Name, fmt.Sprintf("database.server.id %s is also used by %s: two binlog clients with one id disconnect each other", c.DatabaseServerID, strings.Join(without(others, c.Name), ", ")))
			}
		}
		p := c.Stored
		if p == nil {
			continue
		}
		purgedHit := false
		if p.GTIDs != "" && gtidOn {
			stored, err := gtid.Parse(p.GTIDs)
			if err == nil && errPurged == nil {
				if missing := purged.Subtract(stored); !missing.IsEmpty() {
					purgedHit = true
					add(SeverityFault, CodeBinlogPurged, c.Name, fmt.Sprintf("the binlog the connector needs is purged: %d transactions of gtid_purged are not in the stored GTID set (%s), so they were never read; the connector cannot resume without a new snapshot or a reset", missing.Count(), cutGTID(missing.String())))
				}
			}
			if err == nil && errExec == nil && !executed.Contains(stored) {
				ahead := stored.Subtract(executed)
				add(SeverityFault, CodePastEnd, c.Name, fmt.Sprintf("position past the end: the stored GTID set holds %d transactions this server has not executed (%s): the offset is from another server or a restored backup", ahead.Count(), cutGTID(ahead.String())))
			}
		} else if p.File != "" {
			base, num, ok := splitBinlog(p.File)
			switch {
			case !ok:
			case oldestOK && base != oldestBase || !oldestOK && curOK && base != curBase:
				want := oldestBase
				if !oldestOK {
					want = curBase
				}
				add(SeverityWarning, CodeOtherServer, c.Name, fmt.Sprintf("the stored position is from another server: stored file %s has base name %q, this server writes %q", p.File, base, want))
			default:
				if oldestOK && num < oldestNum {
					purgedHit = true
					add(SeverityFault, CodeBinlogPurged, c.Name, fmt.Sprintf("the binlog the connector needs is purged: stored file %s is older than the oldest file the server keeps (%s)", p.File, st.Binlogs[0].Name))
				}
				if curOK && (num > curNum || num == curNum && p.Pos > st.Pos) {
					add(SeverityFault, CodePastEnd, c.Name, fmt.Sprintf("position past the end: stored %s:%d is beyond the server's current %s:%d", p.File, p.Pos, st.File, st.Pos))
				}
			}
			if !gtidOn && p.ServerID != 0 && st.ServerID != 0 && p.ServerID != st.ServerID {
				add(SeverityWarning, CodeOtherServer, c.Name, fmt.Sprintf("the stored position is from another server: the offset's server_id is %d, this server's is %d, and without GTIDs a file and position mean nothing on another server", p.ServerID, st.ServerID))
			}
		}
		if !purgedHit && p.TsSec > 0 && !strings.EqualFold(c.State, "RUNNING") {
			keep, what := retention(st)
			if age := now.Sub(time.Unix(p.TsSec, 0)); keep > 0 && age > keep {
				state := orUnknown(c.State)
				add(SeverityWarning, CodeRetentionShort, c.Name, fmt.Sprintf("retention is shorter than the connector's age: its last stored event is %s old and the connector is %s, but the server keeps binlogs %s (%s); the files it needs are being purged", roundDur(age), state, roundDur(keep), what))
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return sevRank(out[i].Severity) < sevRank(out[j].Severity) })
	return out
}

// retention is how long the server keeps binlogs: RDS retention hours when
// set (RDS purges by it, not by binlog_expire_logs_seconds), else
// binlog_expire_logs_seconds; 0 = not purged by age.
func retention(st mysql.BinlogStatus) (time.Duration, string) {
	if st.RetentionHours != nil && *st.RetentionHours > 0 {
		return time.Duration(*st.RetentionHours) * time.Hour, "RDS binlog retention hours"
	}
	if st.ExpireSeconds > 0 {
		return time.Duration(st.ExpireSeconds) * time.Second, "binlog_expire_logs_seconds"
	}
	return 0, ""
}

func sevRank(s string) int {
	switch s {
	case SeverityFault:
		return 0
	case SeverityWarning:
		return 1
	}
	return 2
}

// result renders the summary: verdicts first, then the server, then each
// connector.
func result(d BinlogStatusData) tools.Result {
	var sb strings.Builder
	if len(d.Verdicts) == 0 {
		sb.WriteString("no binlog problem found for the compared connectors\n")
	}
	for _, v := range d.Verdicts {
		sb.WriteString(strings.ToUpper(v.Severity))
		if v.Connector != "" {
			sb.WriteString(" " + oneLine(v.Connector, 120))
		}
		sb.WriteString(": " + oneLine(v.Text, 600) + "\n")
	}
	s := d.Server
	fmt.Fprintf(&sb, "server: server_id %d, uuid %s, gtid_mode %s; current %s:%d\n", s.ServerID, orUnknown(s.ServerUUID), orUnknown(s.GTIDMode), orUnknown(s.File), s.Pos)
	if n := len(s.Binlogs); n > 0 {
		fmt.Fprintf(&sb, "binlogs kept: %d files, oldest %s, newest %s", n+s.BinlogsOmitted, s.Binlogs[0].Name, s.Binlogs[n-1].Name)
		if s.BinlogsOmitted > 0 {
			fmt.Fprintf(&sb, " (%d not listed)", s.BinlogsOmitted)
		}
		sb.WriteString("\n")
	}
	if keep, what := retention(mysql.BinlogStatus{ExpireSeconds: s.ExpireSeconds, RetentionHours: s.RetentionHours}); keep > 0 {
		fmt.Fprintf(&sb, "retention: %s (%s)\n", roundDur(keep), what)
	} else {
		sb.WriteString("retention: binlogs are not purged by age\n")
	}
	if s.GTIDExecuted != "" || s.GTIDPurged != "" {
		fmt.Fprintf(&sb, "gtid_executed: %s\ngtid_purged: %s\n", cutGTID(gtidText(s.GTIDExecuted)), cutGTID(gtidText(s.GTIDPurged)))
	}
	for _, c := range d.Connectors {
		sb.WriteString(oneLine(c.Name, 120) + ":")
		if c.State != "" {
			sb.WriteString(" " + c.State + ";")
		}
		if c.DatabaseServerID != "" {
			sb.WriteString(" database.server.id " + c.DatabaseServerID + ";")
		}
		if p := c.Stored; p != nil {
			sb.WriteString(" stored")
			if p.File != "" {
				fmt.Fprintf(&sb, " %s:%d", p.File, p.Pos)
			}
			if p.ServerID != 0 {
				fmt.Fprintf(&sb, " server_id %d", p.ServerID)
			}
			if p.TsSec > 0 {
				fmt.Fprintf(&sb, " ts %s", time.Unix(p.TsSec, 0).UTC().Format(time.RFC3339))
			}
			if p.Snapshot {
				sb.WriteString(" (snapshot)")
			}
			if p.GTIDs != "" {
				sb.WriteString(" gtids " + cutGTID(p.GTIDs))
			}
		}
		for _, n := range c.Notes {
			sb.WriteString(" [" + oneLine(n, maxNoteBytes) + "]")
		}
		sb.WriteString("\n")
	}
	for _, n := range d.Notes {
		sb.WriteString("note: " + oneLine(n, maxNoteBytes) + "\n")
	}
	summary, cut := tools.Truncate(sb.String())
	return tools.Result{Summary: summary, Data: d, Truncated: cut}
}

// gtidText prints a server GTID set canonically when it parses, else says
// so; server text is never printed unparsed.
func gtidText(s string) string {
	set, err := gtid.Parse(s)
	if err != nil {
		return "(not a GTID set)"
	}
	if set.IsEmpty() {
		return "(empty)"
	}
	return set.String()
}

func cutGTID(s string) string {
	if len(s) <= maxGTIDSummaryBytes {
		return s
	}
	return s[:maxGTIDSummaryBytes] + "…"
}

// oneLine folds untrusted text (Connect configs and statuses, error
// messages) to one printable line: a line break, a control or format rune
// must not add summary lines of its own. Secrets are masked first.
func oneLine(s string, n int) string {
	s, _ = redact.Text(strings.ToValidUTF8(s, "?"))
	s = strings.Map(func(r rune) rune {
		if (r != ' ' && !unicode.IsPrint(r)) || r == ' ' || r == ' ' {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		i := n
		for i > 0 && !utf8RuneStart(s[i]) {
			i--
		}
		s = s[:i] + "…"
	}
	return s
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

func errText(err error) string { return oneLine(err.Error(), maxNoteBytes) }

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func without(list []string, drop string) []string {
	var out []string
	for _, s := range list {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}

// roundDur prints a duration in whole hours or days, minutes when short.
func roundDur(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%.1f d", d.Hours()/24)
	case d >= time.Hour:
		return fmt.Sprintf("%.1f h", d.Hours())
	default:
		return d.Round(time.Minute).String()
	}
}
