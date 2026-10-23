package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// The two interfaces below are the Q5a shapes from tasks/Q-board.md §5. They
// belong in internal/clients/clients.go (Q0) but are declared here until that
// contract lands; the PR carries a contract change request with the exact
// clients.ConnectLifecycleWriter and clients.SourceOffsetReader to move them.
// Callers assert for them at use time, like the other optional client
// interfaces, so a fake or the eval replay need not grow.
var (
	_ SourceOffsetReader     = (*Client)(nil)
	_ ConnectLifecycleWriter = (*Client)(nil)
	_ ConnectInfoReader      = (*Client)(nil)
)

// SourceOffset is one {partition, offset} pair of a connector's stored
// offsets. Both maps are defined by the connector implementation (for
// Debezium: a partition identifying the slot/server and an offset holding the
// LSN or binlog file and position). They are positions only: the Connect
// offsets API never returns record data.
type SourceOffset struct {
	Partition map[string]any `json:"partition"`
	Offset    map[string]any `json:"offset"` // nil resets the partition's offset in an alter request
}

// SourceOffsetReader reads a connector's stored offsets with
// GET /connectors/{name}/offsets (KIP-875; added in Kafka 3.5). It is
// optional: the tool asserts for it, and a Connect without the endpoint
// answers clients.ErrNotSupported naming the version that adds it.
type SourceOffsetReader interface {
	SourceOffsets(ctx context.Context, connector string) ([]SourceOffset, error)
}

// ConnectLifecycleWriter creates and deletes connectors and stops, resets and
// alters their stored offsets. It is a writer: wiring builds it only when
// write mode is on, and only package actions hold one. Every method takes the
// room's pipeline (tools.WithPipeline) or the explicit tools.WithUnscoped
// opt-out and refuses a connector outside the pipeline before any I/O.
type ConnectLifecycleWriter interface {
	// CreateConnector registers a new connector. A name that already exists
	// answers ErrConnectorExists (HTTP 409).
	CreateConnector(ctx context.Context, name string, cfg map[string]string) error
	// DeleteConnector deletes the connector and stops its tasks. Connect
	// keeps the stored offsets under the name after the deletion.
	DeleteConnector(ctx context.Context, name string) error
	// StopConnector moves the connector to STOPPED and shuts its tasks down
	// (PUT /connectors/{name}/stop, Kafka 3.5+). Reset and alter offsets
	// require this state.
	StopConnector(ctx context.Context, name string) error
	// ResetSourceOffsets clears every stored offset of a STOPPED connector
	// (DELETE /connectors/{name}/offsets, Kafka 3.6+).
	ResetSourceOffsets(ctx context.Context, name string) error
	// AlterSourceOffsets replaces the stored offsets of a STOPPED connector
	// (PATCH /connectors/{name}/offsets, Kafka 3.6+). A nil Offset in an
	// entry resets that partition, as the Kafka docs specify.
	AlterSourceOffsets(ctx context.Context, name string, offsets []SourceOffset) error
}

// ServerInfo is Connect's answer to GET /: the worker's version (the Kafka
// release it ships with), its commit and the Kafka cluster id. It is the Q0
// clients.ConnectServerInfo shape, declared locally for the same reason as the
// interfaces above.
type ServerInfo struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterID string `json:"kafka_cluster_id"`
}

// ConnectInfoReader reads Connect's own version information. It is optional;
// the offsets tool uses it only to name the running version when an endpoint
// is missing.
type ConnectInfoReader interface {
	ServerInfo(ctx context.Context) (ServerInfo, error)
}

// ErrConnectorExists reports a create for a name that is already registered.
var ErrConnectorExists = errors.New("connect: connector already exists")

// ErrConnectorNotStopped reports an offset reset or alter for a connector
// that is not in the STOPPED state.
var ErrConnectorNotStopped = errors.New("connect: connector is not stopped")

// ErrRebalancing reports Connect's 409 for a worker-group rebalance: a
// transient conflict to retry, not a statement about the connector's state or
// name. (Additive to the Q-board shapes; see the PR's contract change
// request.)
var ErrRebalancing = errors.New("connect: worker group is rebalancing; retry")

// The Kafka versions that add each KIP-875 endpoint, from the Apache Kafka
// documentation:
//   - 3.5 (https://kafka.apache.org/35/documentation.html#connect_rest)
//     documents GET /connectors/{name}/offsets and PUT /connectors/{name}/stop;
//   - 3.6 (https://kafka.apache.org/36/documentation.html#connect_rest)
//     adds DELETE and PATCH /connectors/{name}/offsets and states that the
//     connector must be STOPPED for both.
const (
	kip875ReadVersion   = "3.5"
	kip875ModifyVersion = "3.6"

	// maxOffsetEntries caps one alter request. A source connector stores one
	// offset per partition; a thousand is far above any real connector and
	// keeps a bad caller from sending a huge body.
	maxOffsetEntries = 1000

	// maxCreateErrorBody caps the Connect body quoted in a create error.
	maxCreateErrorBody = 512

	// maxAlterBodyBytes caps the encoded alter-offsets request body. The entry
	// count alone does not bound it: one entry's partition or offset map can be
	// arbitrarily large.
	maxAlterBodyBytes = 1 << 20
)

// ServerInfo reads GET /: the worker's Kafka version, commit and cluster id.
// The endpoint is a fixed path, so it skips netsec.CheckPath (which rejects
// "/" by design) and uses roundTrip directly; the request is still recorded
// as a provenance call.
func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error) {
	var out ServerInfo
	start := time.Now()
	err := c.roundTrip(ctx, http.MethodGet, "/", nil, &out)
	tools.Record(ctx, tools.Call{
		Kind:   tools.CallHTTP,
		Target: c.baseURL,
		Text:   "GET /",
		At:     start,
		TookMS: time.Since(start).Milliseconds(),
	})
	if err != nil {
		return ServerInfo{}, safeErr(err)
	}
	return out, nil
}

// SourceOffsets reads the connector's stored offsets (KIP-875,
// GET /connectors/{name}/offsets, Kafka 3.5+). It is read-only and takes no
// pipeline scope, like Status and Config; the calling tool checks scope. A
// Connect without the endpoint answers clients.ErrNotSupported naming the
// version that adds it.
func (c *Client) SourceOffsets(ctx context.Context, connector string) ([]SourceOffset, error) {
	if connector == "" {
		return nil, fmt.Errorf("connect: read source offsets: empty connector name")
	}
	path := "/connectors/" + url.PathEscape(connector) + "/offsets"
	var out struct {
		Offsets []SourceOffset `json:"offsets"`
	}
	if err := c.do(ctx, http.MethodGet, path, &out); err != nil {
		return nil, c.kip875Error(ctx, connector, "read source offsets (GET "+path+")", kip875ReadVersion, err)
	}
	return out.Offsets, nil
}

// CreateConnector registers a connector (POST /connectors). The config is
// never logged, quoted in an error or put in a provenance call; Connect's 409
// means the name is taken.
func (c *Client) CreateConnector(ctx context.Context, name string, cfg map[string]string) error {
	if name == "" {
		return fmt.Errorf("connect: create connector: empty connector name")
	}
	if len(cfg) == 0 {
		return fmt.Errorf("connect: create connector %q: empty config", name)
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	body := struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}{Name: name, Config: cfg}
	if err := c.doBody(ctx, http.MethodPost, "/connectors", body, nil); err != nil {
		return createError(name, err)
	}
	return nil
}

// DeleteConnector deletes a connector and halts its tasks
// (DELETE /connectors/{name}). A missing connector answers clients.ErrNotFound.
// Connect keeps the connector's stored offsets after the deletion, so a later
// create with the same name resumes from them.
func (c *Client) DeleteConnector(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("connect: delete connector: empty connector name")
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name)
	if err := c.do(ctx, http.MethodDelete, path, nil); err != nil {
		if rebalancing(err) {
			return fmt.Errorf("connect: delete connector %q: %w", name, ErrRebalancing)
		}
		return fmt.Errorf("connect: delete connector %q: %w", name, safeErr(err))
	}
	return nil
}

// StopConnector stops the connector and shuts its tasks down
// (PUT /connectors/{name}/stop, Kafka 3.5+). A Connect without the endpoint
// answers clients.ErrNotSupported naming Kafka 3.5.
func (c *Client) StopConnector(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("connect: stop connector: empty connector name")
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name) + "/stop"
	if err := c.do(ctx, http.MethodPut, path, nil); err != nil {
		if rebalancing(err) {
			return fmt.Errorf("connect: stop connector %q: %w", name, ErrRebalancing)
		}
		return c.kip875Error(ctx, name, "stop connector (PUT "+path+")", kip875ReadVersion, err)
	}
	return nil
}

// ResetSourceOffsets clears a stopped connector's stored offsets
// (DELETE /connectors/{name}/offsets, Kafka 3.6+). A running connector answers
// ErrConnectorNotStopped; a Connect without the endpoint answers
// clients.ErrNotSupported naming Kafka 3.6.
func (c *Client) ResetSourceOffsets(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("connect: reset source offsets: empty connector name")
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name) + "/offsets"
	err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		if rebalancing(err) {
			return fmt.Errorf("connect: reset source offsets %q: %w", name, ErrRebalancing)
		}
		if notStopped(err) {
			return fmt.Errorf("connect: reset source offsets %q: %w", name, ErrConnectorNotStopped)
		}
		return c.kip875Error(ctx, name, "reset source offsets (DELETE "+path+")", kip875ModifyVersion, err)
	}
	return nil
}

// AlterSourceOffsets replaces a stopped connector's stored offsets
// (PATCH /connectors/{name}/offsets, Kafka 3.6+). A nil Offset in an entry
// resets that partition, as the Kafka documentation specifies. A running
// connector answers ErrConnectorNotStopped; a Connect without the endpoint
// answers clients.ErrNotSupported naming Kafka 3.6.
func (c *Client) AlterSourceOffsets(ctx context.Context, name string, offsets []SourceOffset) error {
	if name == "" {
		return fmt.Errorf("connect: alter source offsets: empty connector name")
	}
	if len(offsets) == 0 {
		return fmt.Errorf("connect: alter source offsets %q: no offsets; reset every offset instead", name)
	}
	if len(offsets) > maxOffsetEntries {
		return fmt.Errorf("connect: alter source offsets %q: %d entries; at most %d", name, len(offsets), maxOffsetEntries)
	}
	for i, o := range offsets {
		if o.Partition == nil {
			return fmt.Errorf("connect: alter source offsets %q: entry %d has no partition", name, i)
		}
	}
	if err := checkWriteScope(ctx, name); err != nil {
		return err
	}
	ctx, cancel := withWriteDeadline(ctx)
	defer cancel()
	path := "/connectors/" + url.PathEscape(name) + "/offsets"
	body := struct {
		Offsets []SourceOffset `json:"offsets"`
	}{Offsets: offsets}
	// Encode here to enforce the body cap; the error never quotes a value.
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("connect: alter source offsets %q: encode body: %w", name, err)
	}
	if len(b) > maxAlterBodyBytes {
		return fmt.Errorf("connect: alter source offsets %q: request body is %d bytes; at most %d", name, len(b), maxAlterBodyBytes)
	}
	err = c.doBody(ctx, http.MethodPatch, path, json.RawMessage(b), nil)
	if err != nil {
		if rebalancing(err) {
			return fmt.Errorf("connect: alter source offsets %q: %w", name, ErrRebalancing)
		}
		if notStopped(err) {
			return fmt.Errorf("connect: alter source offsets %q: %w", name, ErrConnectorNotStopped)
		}
		return c.kip875Error(ctx, name, "alter source offsets (PATCH "+path+")", kip875ModifyVersion, err)
	}
	return nil
}

// kip875Error turns Connect's answer to an optional KIP-875 endpoint into the
// error the caller needs: a 405, or a 404 on a Connect where the connector
// itself exists, means the endpoint is missing and becomes ErrNotSupported
// with the Kafka version that adds it. A 404 for a connector that also answers
// 404 on GET /connectors/{name} stays ErrNotFound — including when the exists
// probe itself fails (a 500 or a rebalance mid-probe must not turn a missing
// connector into "not supported"). The two requests are not atomic: a
// connector created between them can read as ErrNotSupported once; the next
// call answers correctly.
func (c *Client) kip875Error(ctx context.Context, connector, call, added string, err error) error {
	var se *statusError
	if errors.As(err, &se) && se.Code == http.StatusMethodNotAllowed {
		return kip875NotSupported(call, added)
	}
	if !errors.Is(err, clients.ErrNotFound) {
		return fmt.Errorf("connect: %s: %w", call, safeErr(err))
	}
	exists, xerr := c.connectorExists(ctx, connector)
	if xerr != nil {
		return fmt.Errorf("connect: %s: %w", call, safeErr(err))
	}
	if !exists {
		return fmt.Errorf("connect: %s: %w", call, safeErr(err))
	}
	return kip875NotSupported(call, added)
}

// kip875NotSupported wraps clients.ErrNotSupported with the endpoint and the
// Kafka version that adds it.
func kip875NotSupported(call, added string) error {
	return fmt.Errorf("connect: %s: Kafka Connect %s or later (KIP-875) adds this endpoint: %w",
		call, added, clients.ErrNotSupported)
}

// connectMessage returns the message of Connect's JSON error envelope
// ({"error_code":..,"message":..}), or "" when the body is not that shape.
// Matching the decoded message rather than the raw body keeps an echoed
// request value (Connect quotes the offending input in deserialization
// errors) from passing for Connect's own words.
func connectMessage(se *statusError) string {
	var out struct {
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(se.Body), &out) != nil {
		return ""
	}
	return out.Message
}

// notStopped reports whether err is Connect's refusal to modify the offsets
// of a connector that is not STOPPED. Kafka 3.6 through 4.2 answer 400 with
// BadRequestException ("Connectors must be in the STOPPED state before their
// offsets can be modified", DistributedHerder.modifyConnectorOffsetsChecks);
// the prefix match on the decoded envelope message keeps a 400 that merely
// echoes that phrase inside a request value out. A 409 is the Q-board's state
// conflict — unless its message names a rebalance, a different and transient
// condition (the connector may already be stopped) that the callers report as
// retryable via rebalancing().
func notStopped(err error) bool {
	var se *statusError
	if !errors.As(err, &se) {
		return false
	}
	if se.Code == http.StatusConflict {
		return !rebalanceMessage(se)
	}
	return se.Code == http.StatusBadRequest && strings.HasPrefix(connectMessage(se), "Connectors must be in the STOPPED state")
}

// rebalancing reports whether err is Connect's 409 for a worker-group
// rebalance ("Cannot complete request momentarily due to stale configuration
// (typically caused by a worker rebalance)", "rebalance is expected"): a
// transient condition to retry, not a statement about the connector's state
// or name.
func rebalancing(err error) bool {
	var se *statusError
	return errors.As(err, &se) && se.Code == http.StatusConflict && rebalanceMessage(se)
}

func rebalanceMessage(se *statusError) bool {
	return strings.Contains(strings.ToLower(connectMessage(se)), "rebalance")
}

// connectorExists asks GET /connectors/{name}; it is used only to tell a
// missing KIP-875 route from a missing connector.
func (c *Client) connectorExists(ctx context.Context, name string) (bool, error) {
	var out struct {
		Name string `json:"name"`
	}
	path := "/connectors/" + url.PathEscape(name)
	if err := c.do(ctx, http.MethodGet, path, &out); err != nil {
		if errors.Is(err, clients.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// createError keeps the config out of a create error: Connect echoes request
// values in a validation error, and a config can hold a password. The status
// stays; the body is redacted, folded and capped. A 409 is ErrConnectorExists
// only when Connect says the name is taken; the same status during a worker
// rebalance is a transient conflict, and reporting it as "already exists"
// would make a caller silently skip the create.
func createError(name string, err error) error {
	var se *statusError
	if !errors.As(err, &se) {
		return fmt.Errorf("connect: create connector %q: %w", name, err)
	}
	if se.Code == http.StatusConflict {
		if rebalanceMessage(se) {
			return fmt.Errorf("connect: create connector %q: %w", name, ErrRebalancing)
		}
		return fmt.Errorf("connect: create connector %q: %w", name, ErrConnectorExists)
	}
	return fmt.Errorf("connect: create connector %q: status %d: %s", name, se.Code, sanitizeBody(se.Body))
}

// safeErr rewraps a Connect error for a lifecycle path: a statusError's body
// is redacted, folded and capped before it can reach an error string, a log
// or a prompt — Connect echoes request values (which can hold credentials)
// and arbitrary server text (which can hold control characters) in its error
// bodies. Everything else passes through untouched.
func safeErr(err error) error {
	var se *statusError
	if !errors.As(err, &se) || se.Body == "" {
		return err
	}
	return &statusError{Method: se.Method, Path: se.Path, Code: se.Code, Body: sanitizeBody(se.Body)}
}

// sanitizeBody redacts credentials in a Connect error body, folds control
// characters and other unprintable runes (terminal escapes, Unicode line and
// direction controls) to spaces, and caps the result on a rune boundary.
func sanitizeBody(s string) string {
	s = redactText(s)
	s = strings.Map(func(r rune) rune {
		if r != ' ' && !unicode.IsPrint(r) {
			return ' '
		}
		return r
	}, s)
	if len(s) > maxCreateErrorBody {
		// Cut on a rune boundary: a byte slice can split a UTF-8 rune and put
		// invalid UTF-8 into the error.
		cut := maxCreateErrorBody
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut] + "…"
	}
	return s
}

// redactText masks credentials in a Connect error body before it reaches an
// error, a log or a prompt.
func redactText(s string) string {
	out, _ := redact.Text(s)
	return out
}
