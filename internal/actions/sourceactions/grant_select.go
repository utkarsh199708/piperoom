package sourceactions

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// GrantSelectName is the registered name of the action.
const GrantSelectName = "source_grant_select"

// maxGrantTables caps the tables of one step: a pipeline lists its source
// tables, and a longer list is more likely a mistake than a fix.
const maxGrantTables = 50

// GrantWriter is the admin write of source_grant_select
// (postgres.SourceAdminWriter.GrantSelect, Q2b): SELECT and nothing else.
type GrantWriter interface {
	GrantSelect(ctx context.Context, role string, tables []string) error
}

var _ GrantWriter = (*postgres.Writer)(nil)

// GrantSelectDeps are the clients of source_grant_select.
type GrantSelectDeps struct {
	// Connect reads the connector config, whose database.user is the only
	// role this action grants to. The read-only Connect client is enough.
	Connect tools.ConfigReader
	// Source is the read-only source client; it must implement
	// RoleInfoReader (Q2a).
	Source clients.SourceDB
	// Writer is used by Execute only; nil (untyped) when write mode is off
	// or SOURCE_DB_WRITE_URL is empty, and then Execute refuses.
	Writer GrantWriter

	// timeout overrides opTimeout; tests only.
	timeout time.Duration
}

// NewGrantSelect returns source_grant_select: GRANT SELECT ON <tables> TO
// <the connector's database.user>.
func NewGrantSelect(d GrantSelectDeps) actions.Action { return &grantSelect{d: d} }

type grantSelect struct{ d GrantSelectDeps }

var (
	_ actions.Scoped  = (*grantSelect)(nil)
	_ actions.Preview = (*grantSelect)(nil)
)

func (a *grantSelect) Name() string  { return GrantSelectName }
func (a *grantSelect) Layer() string { return layerSource }

func (a *grantSelect) Describe() string {
	return "Grant SELECT, and nothing else, on source tables of the pipeline to its Debezium connector's database user (GRANT SELECT ON <tables> TO <database.user>). " +
		"Use it when a snapshot or a newly captured table fails with permission denied. " +
		"Refused unless role is exactly the connector's database.user, every table is one of the pipeline's source_tables, the role exists and lacks SELECT on at least one of them."
}

func (a *grantSelect) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string", "description": "The pipeline's Debezium source connector whose database.user gets the grant."},
			"role":      map[string]any{"type": "string", "description": "That connector's database.user, exactly.", "pattern": identRe.String()},
			"tables": map[string]any{
				"type": "array", "minItems": 1, "maxItems": maxGrantTables, "uniqueItems": true,
				"items":       map[string]any{"type": "string", "pattern": tablePattern},
				"description": "Source tables, \"schema.table\", each one of the pipeline's source_tables.",
			},
		},
		"additionalProperties": false,
		"required":             []string{"connector", "role", "tables"},
	}
}

type grantParams struct {
	Connector string   `json:"connector"`
	Role      string   `json:"role"`
	Tables    []string `json:"tables"`
}

func parseGrant(raw json.RawMessage) (grantParams, error) {
	var p grantParams
	if err := decodeStrict(GrantSelectName, raw, &p); err != nil {
		return p, err
	}
	if err := checkConnectorName(GrantSelectName, p.Connector); err != nil {
		return p, err
	}
	if !identRe.MatchString(p.Role) {
		return p, fmt.Errorf("sourceactions: %s: invalid role %q (letters, digits and _, not starting with a digit, at most 63)", GrantSelectName, cut(p.Role))
	}
	// The writer refuses these too; refusing here keeps the step from ever
	// reaching an approver.
	if r := strings.ToLower(p.Role); strings.HasPrefix(r, "pg_") || slices.Contains([]string{"public", "current_user", "current_role", "session_user"}, r) {
		return p, fmt.Errorf("sourceactions: %s: %q is a reserved or predefined role, never a connector's database.user", GrantSelectName, p.Role)
	}
	if len(p.Tables) == 0 || len(p.Tables) > maxGrantTables {
		return p, fmt.Errorf("sourceactions: %s: tables needs 1 to %d entries, got %d", GrantSelectName, maxGrantTables, len(p.Tables))
	}
	seen := make(map[string]bool, len(p.Tables))
	for _, t := range p.Tables {
		if err := checkTable(GrantSelectName, t); err != nil {
			return p, err
		}
		if seen[t] {
			return p, fmt.Errorf("sourceactions: %s: table %s is listed twice", GrantSelectName, t)
		}
		seen[t] = true
	}
	return p, nil
}

// CheckScope refuses, without I/O, a connector or table outside pl and a
// pipeline that is not a CDC one.
func (a *grantSelect) CheckScope(pl owners.Pipeline, raw json.RawMessage) error {
	p, err := parseGrant(raw)
	if err != nil {
		return err
	}
	return grantInScope(pl, p)
}

func grantInScope(pl owners.Pipeline, p grantParams) error {
	if err := pipelineScope(GrantSelectName, pl, p.Connector); err != nil {
		return err
	}
	return sourceTablesScope(GrantSelectName, pl, p.Tables...)
}

// checkConnectorUser reads the connector config and refuses a role that is
// not its database.user: the action fixes the connector's own access, never
// another role's.
func (a *grantSelect) checkConnectorUser(ctx context.Context, p grantParams) error {
	if a.d.Connect == nil {
		return fmt.Errorf("sourceactions: %s: no Connect client to read connector %q's database.user", GrantSelectName, p.Connector)
	}
	cfg, err := a.d.Connect.Config(ctx, p.Connector)
	if err != nil {
		return fmt.Errorf("sourceactions: %s: read connector %q config: %w", GrantSelectName, p.Connector, err)
	}
	if !isDebeziumSource(cfg["connector.class"]) {
		return fmt.Errorf("sourceactions: %s: connector %q is not a Debezium source connector (connector.class %q)", GrantSelectName, p.Connector, cut(cfg["connector.class"]))
	}
	user := cfg["database.user"]
	switch {
	case strings.TrimSpace(user) == "":
		return fmt.Errorf("sourceactions: %s: connector %q sets no database.user", GrantSelectName, p.Connector)
	case configRef(user):
		return fmt.Errorf("sourceactions: %s: connector %q's database.user is a config provider reference, so the role cannot be checked against it; a DBA grants it", GrantSelectName, p.Connector)
	case user != p.Role:
		return fmt.Errorf("sourceactions: %s: role %q is not connector %q's database.user (%q); only that role may be granted", GrantSelectName, p.Role, p.Connector, oneLine(user, 64))
	}
	return nil
}

func (a *grantSelect) roleInfo(ctx context.Context, p grantParams) (RoleInfo, error) {
	r, ok := a.d.Source.(RoleInfoReader)
	if !ok {
		return RoleInfo{}, fmt.Errorf("sourceactions: %s: the source client cannot read roles (no RoleInfo)", GrantSelectName)
	}
	info, err := r.RoleInfo(ctx, p.Role, p.Tables)
	if err != nil {
		return RoleInfo{}, fmt.Errorf("sourceactions: %s: read role %q: %w", GrantSelectName, p.Role, err)
	}
	return info, nil
}

// missing lists the tables the role cannot SELECT yet, in params order.
func missing(p grantParams, info RoleInfo) []string {
	var out []string
	for _, t := range p.Tables {
		if !info.Selectable[t] {
			out = append(out, t)
		}
	}
	return out
}

// checkRole refuses a missing role and a grant that changes nothing. On
// MySQL it also refuses root and a user name with more than one account
// before approval (design §8.1): a guess between user@% and user@10.0.% could
// widen the wrong account, so a DBA grants it.
func checkRole(kind string, p grantParams, info RoleInfo) error {
	if isMySQL(kind) && strings.EqualFold(p.Role, "root") {
		return fmt.Errorf("sourceactions: %s: %q is MySQL's administrator, never a connector's database.user", GrantSelectName, p.Role)
	}
	if !info.Exists {
		return fmt.Errorf("sourceactions: %s: role %q does not exist on the source: %w", GrantSelectName, p.Role, clients.ErrNotFound)
	}
	if isMySQL(kind) {
		switch len(info.Accounts) {
		case 0:
			return fmt.Errorf("sourceactions: %s: the source named no account of user %q, so the grant's account cannot be shown to the approvers", GrantSelectName, p.Role)
		case 1:
		default:
			accts := make([]string, len(info.Accounts))
			for i, ac := range info.Accounts {
				accts[i] = mysqlAccount(ac)
			}
			return fmt.Errorf("sourceactions: %s: user %q has accounts %s; grant by hand or remove one (a guess could widen the wrong account)", GrantSelectName, p.Role, strings.Join(accts, ", "))
		}
	}
	if len(missing(p, info)) == 0 {
		return fmt.Errorf("sourceactions: %s: role %q can already SELECT every listed table; nothing to do", GrantSelectName, p.Role)
	}
	return nil
}

// Validate refuses bad params, a target outside the room's pipeline (when
// ctx carries it), a role that is not the connector's database.user, a
// missing role and a grant that changes nothing. A missing writer is refused
// at Execute only (P0 ruling #606).
func (a *grantSelect) Validate(ctx context.Context, raw json.RawMessage) error {
	p, err := parseGrant(raw)
	if err != nil {
		return err
	}
	if err := validateScope(ctx, func(pl owners.Pipeline) error { return grantInScope(pl, p) }); err != nil {
		return err
	}
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	if err := a.checkConnectorUser(ctx, p); err != nil {
		return err
	}
	info, err := a.roleInfo(ctx, p)
	if err != nil {
		return err
	}
	return checkRole(stepKind(ctx, a.d.Source), p, info)
}

// Preview reads the connector's database.user and the role's privileges on
// read-only clients. It never holds the writer.
func (a *grantSelect) Preview(ctx context.Context, raw json.RawMessage) (actions.PreviewResult, error) {
	p, err := parseGrant(raw)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	if _, _, err := ctxScope(ctx, GrantSelectName, func(pl owners.Pipeline) error { return grantInScope(pl, p) }); err != nil {
		return actions.PreviewResult{}, err
	}
	ctx, cancel := withTimeout(ctx, a.d.timeout)
	defer cancel()
	if err := a.checkConnectorUser(ctx, p); err != nil {
		return actions.PreviewResult{}, err
	}
	info, err := a.roleInfo(ctx, p)
	if err != nil {
		return actions.PreviewResult{}, err
	}
	kind := stepKind(ctx, a.d.Source)
	if isMySQL(kind) {
		return mysqlGrantPreview(p, info), nil
	}
	return grantPreview(p, info), nil
}

// mysqlGrantPreview is the MySQL preview: it names the one account the
// grant goes to (user@host, resolved from mysql.user) and the further
// privileges the connector needs, which this action never grants.
func mysqlGrantPreview(p grantParams, info RoleInfo) actions.PreviewResult {
	res := actions.PreviewResult{
		Target: fmt.Sprintf("user %s (connector %s's database.user) on the source MySQL", p.Role, p.Connector),
	}
	var has, lacks []string
	for _, t := range p.Tables {
		if info.Selectable[t] {
			has = append(has, t)
		} else {
			lacks = append(lacks, t)
		}
	}
	var cur strings.Builder
	if !info.Exists {
		fmt.Fprintf(&cur, "user %s has no account", p.Role)
	} else {
		accts := make([]string, len(info.Accounts))
		for i, ac := range info.Accounts {
			accts[i] = mysqlAccount(ac)
		}
		fmt.Fprintf(&cur, "user %s: %s %s (login %s, REPLICATION SLAVE and CLIENT %s)", p.Role,
			tools.Plural(len(accts), "account", "accounts"), strings.Join(accts, ", "), yesNo(info.CanLogin), yesNo(info.Replication))
		if len(has) > 0 {
			fmt.Fprintf(&cur, "; can SELECT %s", strings.Join(has, ", "))
		}
		if len(lacks) > 0 {
			fmt.Fprintf(&cur, "; cannot SELECT %s", strings.Join(lacks, ", "))
		}
		if l := lackingPrivileges(info.Privileges); l != "" {
			fmt.Fprintf(&cur, "; lacks %s (not granted by this step)", l)
		}
	}
	res.Current = cur.String()
	if err := checkRole(owners.SourceKindMySQL, p, info); err != nil {
		res.Expected = "no change: this step is refused (" + strings.TrimPrefix(err.Error(), "sourceactions: "+GrantSelectName+": ") + ")"
		return res
	}
	account := mysqlAccount(info.Accounts[0])
	res.Target = fmt.Sprintf("account %s (connector %s's database.user) on the source MySQL", account, p.Connector)
	var b strings.Builder
	fmt.Fprintf(&b, "GRANT SELECT ON %s TO %s, one statement per table: SELECT only, no GRANT OPTION, no other privilege, this account only; %d of %d tables gain it.",
		quoteTablesFor(owners.SourceKindMySQL, p.Tables), account, len(lacks), len(p.Tables))
	if !info.CanLogin {
		b.WriteString(" The account is locked or its password expired, so the connector still fails until a DBA unlocks it.")
	}
	b.WriteString(" A failed snapshot or task needs a restart (connect_restart_task) to read the tables.")
	res.Expected = b.String()
	res.Reversible = true
	res.Rollback = fmt.Sprintf("REVOKE SELECT ON %s FROM %s (by a DBA; no registered action; only the tables that lacked it)", quoteTablesFor(owners.SourceKindMySQL, lacks), account)
	return res
}

// lackingPrivileges lists, sorted, the global privileges RoleInfo reports as
// missing ("" when none or none reported).
func lackingPrivileges(privs map[string]bool) string {
	var out []string
	for name, ok := range privs {
		if !ok {
			out = append(out, oneLine(name, 32))
		}
	}
	slices.Sort(out)
	return strings.Join(out, ", ")
}

func grantPreview(p grantParams, info RoleInfo) actions.PreviewResult {
	res := actions.PreviewResult{
		Target: fmt.Sprintf("role %s (connector %s's database.user) on the source database", p.Role, p.Connector),
	}
	var has, lacks []string
	for _, t := range p.Tables {
		if info.Selectable[t] {
			has = append(has, t)
		} else {
			lacks = append(lacks, t)
		}
	}
	var cur strings.Builder
	if !info.Exists {
		fmt.Fprintf(&cur, "role %s does not exist", p.Role)
	} else {
		fmt.Fprintf(&cur, "role %s exists (login %s, replication %s)", p.Role, yesNo(info.CanLogin), yesNo(info.Replication))
		if len(has) > 0 {
			fmt.Fprintf(&cur, "; can SELECT %s", strings.Join(has, ", "))
		}
		if len(lacks) > 0 {
			fmt.Fprintf(&cur, "; cannot SELECT %s", strings.Join(lacks, ", "))
		}
	}
	res.Current = cur.String()
	if err := checkRole(owners.SourceKindPostgres, p, info); err != nil {
		res.Expected = "no change: this step is refused (" + strings.TrimPrefix(err.Error(), "sourceactions: "+GrantSelectName+": ") + ")"
		return res
	}
	var b strings.Builder
	fmt.Fprintf(&b, "GRANT SELECT ON TABLE %s TO %s: SELECT only, no GRANT OPTION, no other privilege; %d of %d tables gain it.", quoteTables(p.Tables), quoteIdent(p.Role), len(lacks), len(p.Tables))
	if info.Exists && !info.CanLogin {
		b.WriteString(" The role cannot log in, so the connector still fails until a DBA allows LOGIN.")
	}
	b.WriteString(" A failed snapshot or task needs a restart (connect_restart_task) to read the tables.")
	res.Expected = b.String()
	res.Reversible = true
	res.Rollback = fmt.Sprintf("REVOKE SELECT ON TABLE %s FROM %s (by a DBA; no registered action; only the tables that lacked it)", quoteTables(lacks), quoteIdent(p.Role))
	return res
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// Execute grants through the admin writer, after the scope gate. The grant is
// idempotent, so it covers every listed table.
func (a *grantSelect) Execute(ctx context.Context, raw json.RawMessage) (string, error) {
	p, err := parseGrant(raw)
	if err != nil {
		return "", err
	}
	if _, _, err := ctxScope(ctx, GrantSelectName, func(pl owners.Pipeline) error { return grantInScope(pl, p) }); err != nil {
		return "", err
	}
	if a.d.Writer == nil {
		return "", fmt.Errorf("sourceactions: %s: %w", GrantSelectName, errNoWriter)
	}
	// The connector config may have changed since approval: the grant goes
	// only to the role that is its database.user now.
	rctx, cancel := withTimeout(ctx, a.d.timeout)
	err = a.checkConnectorUser(rctx, p)
	cancel()
	if err != nil {
		return "", err
	}
	if err := a.d.Writer.GrantSelect(ctx, p.Role, p.Tables); err != nil {
		return "", fmt.Errorf("sourceactions: %s: %w", GrantSelectName, err)
	}
	return fmt.Sprintf("granted SELECT on %s to %s. Restart the failed task or snapshot so the connector reads them.", strings.Join(p.Tables, ", "), p.Role), nil
}
