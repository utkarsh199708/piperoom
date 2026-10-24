package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/setup/errcode"
	"piperoom/internal/setup/protocol"
)

// OpSourcePublicationTables is the T0 read behind remoteclients'
// Source.PublicationTables (the U1-9 Postgres driver's publication check).
const OpSourcePublicationTables = "source_publication_tables"

// maxPublicationTables caps the tables one request may name: a pipeline's
// source tables, not a catalogue scan.
const maxPublicationTables = 1000

// maxIdentBytes is PostgreSQL's NAMEDATALEN-1: a longer identifier is
// truncated by the server, so it could match another object's name.
const maxIdentBytes = 63

// identRe is AGENTS.md §4 rule 6's identifier rule. Every name in this op's
// params is checked against it before any read.
var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ErrNoSource: the runner was built without a source database reader, so a
// source read cannot run here.
var ErrNoSource = errors.New("ops: no source database connection on this runner")

// SourceReader is the source Postgres read surface the runner's source ops
// call. *postgres.DB provides it on its read-only pool; Config.Source is the
// seam through which the runner's own source connection plugs in, so the op
// calls the same client method the server-side tools call.
type SourceReader interface {
	PublicationTables(ctx context.Context, publication, role string, tables []string) ([]postgres.PublicationTable, error)
}

var _ SourceReader = (*postgres.DB)(nil)

// sourcePublicationTablesOp reads a publication's membership and a role's
// SELECT privilege on each named table.
type sourcePublicationTablesOp struct{}

// publicationTablesParams is the op's wire shape; remoteclients sends the same
// three fields.
type publicationTablesParams struct {
	Publication string   `json:"publication"`
	Role        string   `json:"role"`
	Tables      []string `json:"tables"`
}

func (sourcePublicationTablesOp) Name() string   { return OpSourcePublicationTables }
func (sourcePublicationTablesOp) Target() string { return "postgres" }
func (sourcePublicationTablesOp) Tier() string   { return protocol.TierT0 }
func (sourcePublicationTablesOp) Layer() string  { return "postgres" }
func (sourcePublicationTablesOp) Describe() string {
	return "read a publication's tables and a role's SELECT privilege on each source table"
}

func (sourcePublicationTablesOp) ParamsSchema() map[string]any {
	ident := map[string]any{"type": "string", "pattern": identRe.String(), "maxLength": maxIdentBytes}
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"publication"},
		"properties": map[string]any{
			"publication": ident,
			"role":        ident,
			"tables": map[string]any{
				"type": "array", "maxItems": maxPublicationTables,
				"items": map[string]any{"type": "string", "pattern": `^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`},
			},
		},
	}
}

func (sourcePublicationTablesOp) Validate(params json.RawMessage, env Env) error {
	_, err := decodePublicationTables(params)
	return err
}

// checkTarget is the runner's TargetChecker: a room that registered no
// postgres target has no source this op may read, so the request is refused
// with RefuseTarget before the guard; bad params are RefuseSchema.
func (sourcePublicationTablesOp) checkTarget(params json.RawMessage, env Env) error {
	if _, err := decodePublicationTables(params); err != nil {
		return err
	}
	return requirePostgresTarget(env)
}

func (op sourcePublicationTablesOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := decodePublicationTables(params)
	if err != nil {
		return Result{}, err
	}
	if err := requirePostgresTarget(env); err != nil {
		return Result{}, err
	}
	if env.Source == nil {
		return Result{
			Summary: "no source database connection on this runner",
			Data:    map[string]string{"error_code": errcode.NotSupported},
		}, ErrNoSource
	}
	tables, err := env.Source.PublicationTables(ctx, p.Publication, p.Role, p.Tables)
	if errors.Is(err, clients.ErrNotFound) {
		// The driver reads a missing publication as the check's failure, not
		// a read error; the code carries that across the wire.
		return Result{
			Summary: fmt.Sprintf("publication %s does not exist", p.Publication),
			Data:    map[string]string{"error_code": errcode.NotFound},
		}, fmt.Errorf("ops: %s: publication %s: %w", op.Name(), p.Publication, err)
	}
	if err != nil {
		return Result{}, fmt.Errorf("ops: %s: %w", op.Name(), err)
	}
	if tables == nil {
		tables = []postgres.PublicationTable{}
	}
	members := 0
	for _, t := range tables {
		if t.InPublication {
			members++
		}
	}
	return Result{
		Summary: fmt.Sprintf("publication %s: %d tables listed, %d in the publication", p.Publication, len(tables), members),
		Data:    map[string]any{"tables": tables},
	}, nil
}

// decodePublicationTables decodes and checks the params without I/O: the
// publication is required, the role optional, and every name passes the
// identifier rule (tables as "schema.table").
func decodePublicationTables(raw json.RawMessage) (publicationTablesParams, error) {
	var p publicationTablesParams
	if err := decodeParams(raw, &p); err != nil {
		return p, err
	}
	if err := checkIdent("publication", p.Publication); err != nil {
		return p, err
	}
	if p.Role != "" {
		if err := checkIdent("role", p.Role); err != nil {
			return p, err
		}
	}
	if len(p.Tables) > maxPublicationTables {
		return p, fmt.Errorf("%w: %d tables, at most %d", ErrParams, len(p.Tables), maxPublicationTables)
	}
	for _, t := range p.Tables {
		schema, table, ok := strings.Cut(t, ".")
		if !ok {
			return p, fmt.Errorf("%w: table %q is not \"schema.table\"", ErrParams, clip(t))
		}
		if err := checkIdent("table schema", schema); err != nil {
			return p, err
		}
		if err := checkIdent("table", table); err != nil {
			return p, err
		}
	}
	return p, nil
}

func checkIdent(field, v string) error {
	if v == "" {
		return fmt.Errorf("%w: %s is required", ErrParams, field)
	}
	if len(v) > maxIdentBytes || !identRe.MatchString(v) {
		return fmt.Errorf("%w: %s %q is not an identifier (%s, at most %d bytes)", ErrParams, field, clip(v), identRe.String(), maxIdentBytes)
	}
	return nil
}

// requirePostgresTarget refuses a source read in a room that registered no
// postgres target: the runner reaches only what the room registered (§2.6).
func requirePostgresTarget(env Env) error {
	for _, t := range env.Targets {
		if strings.EqualFold(t.Kind, "postgres") {
			return nil
		}
	}
	return fmt.Errorf("%w: the room registered no postgres target", ErrTarget)
}
