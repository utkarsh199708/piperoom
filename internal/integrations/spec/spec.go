// Package spec holds the shared types of the integrations feature: the catalog
// (Kind, Field), the test checklist (Check), the stored record (Integration),
// the Store and the Service the HTTP API calls. Design:
// docs/design/integrations.md §4–§5 and docs/design/secret-providers.md.
//
// CONTRACT FILE — owned by the leads. Do not edit; request changes in your PR.
//
// It depends only on the standard library and netsec, so internal/secrets,
// internal/integrations, internal/store, internal/httpapi and internal/wiring
// can all import it without cycles.
package spec

import (
	"context"
	"time"

	"piperoom/internal/clients/netsec"
)

// DefaultWorkspace is the workspace id of a self-hosted install (one
// workspace). Hosted installs have one workspace per customer.
const DefaultWorkspace = "default"

// Category groups kinds that provide the same client. In v1 a workspace has
// at most one integration per category, except CategorySecretManager.
type Category string

// Categories.
const (
	CategorySourceDB      Category = "source_db"      // Build returns clients.SourceDB
	CategoryKafka         Category = "kafka"          // Build returns clients.KafkaAdmin (and clients.DLQReader when it can)
	CategoryConnect       Category = "connect"        // Build returns clients.ConnectClient
	CategoryRegistry      Category = "registry"       // Build returns clients.SchemaRegistryClient
	CategoryWarehouse     Category = "warehouse"      // Build returns clients.Warehouse
	CategoryModel         Category = "model"          // Build returns *agent.Endpoint
	CategoryAlerts        Category = "alerts"         // Build returns nil; the row holds the webhook secret
	CategoryChat          Category = "chat"           // Build returns the Slack bridge settings
	CategorySecretManager Category = "secret_manager" // Build returns a secrets.Provider
)

// Categories lists every category in catalog order.
var Categories = []Category{
	CategorySourceDB, CategoryKafka, CategoryConnect, CategoryRegistry, CategoryWarehouse,
	CategoryModel, CategoryAlerts, CategoryChat, CategorySecretManager,
}

// Valid reports whether c is a known category.
func (c Category) Valid() bool {
	for _, k := range Categories {
		if c == k {
			return true
		}
	}
	return false
}

// UniqueKey is the value a workspace may hold only once (Store.Create and
// Update answer ErrDuplicate on a clash): the category for most categories;
// "alerts:<kind>" for alerts (one row per alert source); for secret managers
// "secret_manager:<name>", where name is Config[ProviderNameField], so a
// workspace may connect several managers under different names.
func UniqueKey(rec Integration) string {
	switch rec.Category {
	case CategoryAlerts:
		return string(rec.Category) + ":" + rec.Kind
	case CategorySecretManager:
		name, _ := rec.Config[ProviderNameField].(string)
		return string(rec.Category) + ":" + name
	default:
		return string(rec.Category)
	}
}

// ProviderNameField is the required text field of every secret_manager kind:
// the provider id that secret:// references name ("vault-corp" in
// secret://vault-corp/database/creds/piperoom-ro#password). It matches
// ProviderNamePattern and is unique per workspace.
const ProviderNameField = "name"

// ProviderNamePattern is the RE2 pattern of a provider id
// (docs/design/secret-providers.md §2).
const ProviderNamePattern = `^[a-z0-9][a-z0-9_-]{0,62}$`

// ProviderFiles is the built-in mounted-files provider id. It is always
// available and needs no row: secret://files/run/secrets/kafka/password.
// A secret_manager row may not use this name.
const ProviderFiles = "files"

// FieldType is how a form input is rendered and stored.
type FieldType string

// Field types. FieldSecret, FieldPEM and FieldJSONFile are secret: write-only
// through the API, sealed at rest or held as a secret:// reference.
const (
	FieldText     FieldType = "text"
	FieldSecret   FieldType = "secret"
	FieldNumber   FieldType = "number"
	FieldSelect   FieldType = "select"
	FieldBool     FieldType = "bool"
	FieldPEM      FieldType = "pem"       // PEM text (CA, client certificate, private key)
	FieldJSONFile FieldType = "json_file" // an uploaded JSON file (e.g. a Google service-account key)
)

// IsSecret reports whether values of this type are secrets.
func (t FieldType) IsSecret() bool {
	return t == FieldSecret || t == FieldPEM || t == FieldJSONFile
}

// Field is one form input. Secret fields are write-only through the API.
type Field struct {
	Name     string    `json:"name"`  // key in Values; ^[a-z][a-z0-9_]*$
	Label    string    `json:"label"` // shown next to the input
	Help     string    `json:"help"`  // one line under the input; may be ""
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	Default  string    `json:"default"` // "" = none; never set for secret fields
	Options  []string  `json:"options"` // FieldSelect only; [] otherwise
	// Pattern is a Go regexp (RE2) the server checks on save and test; the
	// browser check is only a hint. "" = no pattern.
	Pattern string `json:"pattern"`
}

// CheckStatus is the outcome of one check.
type CheckStatus string

// Check outcomes. Only CheckFail blocks a save.
const (
	CheckPass    CheckStatus = "pass"
	CheckWarn    CheckStatus = "warn"
	CheckFail    CheckStatus = "fail"
	CheckSkipped CheckStatus = "skipped"
)

// Check is one line of the test checklist. Detail and Fix never contain a
// secret value or a full record value.
type Check struct {
	ID     string      `json:"id"`    // stable, e.g. "reach", "login", "read_only", "slots_visible"
	Title  string      `json:"title"` // e.g. "Role cannot INSERT into public.orders"
	Status CheckStatus `json:"status"`
	Detail string      `json:"detail"` // what was found, short; "" when nothing to add
	Fix    string      `json:"fix"`    // the exact fix text (SQL, CLI line, setting); "" on pass
}

// Failed reports whether any check failed.
func Failed(checks []Check) bool {
	for _, c := range checks {
		if c.Status == CheckFail {
			return true
		}
	}
	return false
}

// Values are the inputs of one integration with every secret:// reference
// already resolved. Config holds the non-secret fields as text (numbers in
// decimal, bools as "true"/"false"); Secrets holds secret fields.
type Values struct {
	Config  map[string]string
	Secrets map[string]netsec.Secret
}

// Get returns a non-secret field ("" when unset).
func (v Values) Get(name string) string { return v.Config[name] }

// Secret returns a secret field ("" when unset).
func (v Values) Secret(name string) netsec.Secret { return v.Secrets[name] }

// Kind describes one integration card: its form, how to test it and how to
// build its client. Kinds register in internal/integrations.
type Kind struct {
	ID       string   // "postgres-source", "kafka-confluent-cloud", "secrets-vault", ...; ^[a-z0-9][a-z0-9-]*$
	Category Category //
	Title    string   // card title, e.g. "Confluent Cloud"
	Summary  string   // one line on the card
	Logo     string   // logo id the web app maps to an icon, e.g. "postgres"; "" = category icon
	Reads    []string // what Piperoom reads, shown before save
	Never    []string // what Piperoom never does, e.g. "never writes to a table"
	Fields   []Field
	// Required marks the categories needed for the first room (setup checklist).
	Required bool
	// SetupScript returns the least-privilege grant script (SQL, IAM JSON,
	// policy HCL, YAML) for the given non-secret values; v.Secrets is empty.
	// nil = the card has no script.
	SetupScript func(v Values) string
	// Test runs the checks on unsaved values. It never writes to the target.
	// Each check has its own deadline (5 s); the whole test at most 20 s.
	Test func(ctx context.Context, v Values) []Check
	// Build constructs the client (see the Category consts for its type).
	// close releases pools and connections; it is never nil when err is nil.
	Build func(ctx context.Context, v Values) (client any, close func(), err error)
}

// Resolver turns a secret:// reference into its value. internal/secrets
// implements it; integrations depend on this interface, not on that package.
// It must refuse a reference naming a provider of another workspace.
type Resolver interface {
	Resolve(ctx context.Context, workspaceID, ref string) (netsec.Secret, error)
}

// RefKey is the JSON key that marks a secret field held as a reference in
// Integration.Config and in API bodies: {"$ref": "secret://vault-corp/db#password"}.
const RefKey = "$ref"

// RefPrefix starts every secret reference.
const RefPrefix = "secret://"

// Record sources.
const (
	SourceDB  = "db"  // saved through the Integrations page
	SourceEnv = "env" // from environment variables; read-only in the UI
)

// ScopeAll is Integration.Scope for a workspace-wide integration (v1 only).
const ScopeAll = "*"

// Integration is one stored integration (a row of the integrations table).
// It has no JSON tags: the API never encodes it directly (see View).
type Integration struct {
	ID          string   // "int_<random>"; env rows: "env_<category>"
	WorkspaceID string   // DefaultWorkspace when self-hosted
	Category    Category //
	Kind        string   // Kind.ID
	Scope       string   // ScopeAll in v1; a pipeline id later
	// Config holds the non-secret fields; a secret field held as a reference
	// is {"$ref": "secret://..."} under its field name. Never a secret value.
	Config map[string]any
	// Sealed is AES-256-GCM nonce||ciphertext over a JSON object of the typed-in
	// secret values, with additional data "<workspace_id>/<id>/<category>". nil = none.
	Sealed []byte
	// KeyID names the key that sealed Sealed: first 8 hex chars of SHA-256 of the key.
	KeyID string
	// SecretsSet lists the secret field names that have a value (typed in or a reference), sorted.
	SecretsSet []string
	Version    int64 // starts at 1; +1 on every Update and SetEnabled
	Enabled    bool
	LastTest   []Check   // nil = never tested
	LastTestAt time.Time // zero = never tested
	UpdatedBy  string    // user id
	UpdatedAt  time.Time
	Source     string // SourceDB or SourceEnv
}

// AuditAction is what an audit row records.
type AuditAction string

// Audit actions.
const (
	AuditCreated       AuditAction = "created"
	AuditUpdated       AuditAction = "updated"
	AuditTested        AuditAction = "tested"
	AuditEnabled       AuditAction = "enabled"
	AuditDisabled      AuditAction = "disabled"
	AuditDeleted       AuditAction = "deleted"
	AuditSecretRotated AuditAction = "secret_rotated"
)

// AuditEntry is one row of integration_audit. Changed holds field NAMES only.
type AuditEntry struct {
	Seq           int64       `json:"seq"`
	WorkspaceID   string      `json:"workspace_id"`
	IntegrationID string      `json:"integration_id"`
	Action        AuditAction `json:"action"`
	Actor         string      `json:"actor"`
	Changed       []string    `json:"changed"`
	At            time.Time   `json:"at"`
}
