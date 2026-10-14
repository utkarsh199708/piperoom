package spec

import (
	"context"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/clients/netsec"
)

// Service errors. The HTTP API maps them to the codes in docs/api.md
// ("Integrations"); callers compare with errors.Is / errors.As.
var (
	// ErrUnknownKind: the kind id is not in the catalog → 400 validation
	// (404 not_found on the setup-script route, where the kind is in the path).
	ErrUnknownKind = errors.New("integrations: unknown kind")
	// ErrSecretKeyMissing: a typed-in secret must be sealed but
	// PIPEROOM_SECRET_KEY is not set → 409 secret_key_missing.
	ErrSecretKeyMissing = errors.New("integrations: PIPEROOM_SECRET_KEY is not set")
	// ErrManagedByEnv: a write to a category configured by environment
	// variables (or to an env row) → 409 managed_by_env.
	ErrManagedByEnv = errors.New("integrations: managed by environment")
	// ErrRateLimited: more than 10 tests a minute by one admin → 429 rate_limited.
	ErrRateLimited = errors.New("integrations: too many tests")
	// ErrNotSecretManager: a secrets route on a row whose category is not
	// secret_manager → 400 validation.
	ErrNotSecretManager = errors.New("integrations: not a secret manager")
	// ErrListUnsupported: the provider cannot list names → 200 with
	// "list_supported": false (the UI shows a text field).
	ErrListUnsupported = errors.New("integrations: provider cannot list secrets")
	// ErrProviderUnreachable: the secret manager did not answer → 502 provider_unreachable.
	ErrProviderUnreachable = errors.New("integrations: secret manager unreachable")
	// ErrUnknownAlertSource: not alertmanager|grafana|pagerduty|datadog|generic → 404 not_found.
	ErrUnknownAlertSource = errors.New("integrations: unknown alert source")
	// ErrSecretKeyMismatch: a row's sealed secrets open with neither
	// PIPEROOM_SECRET_KEY nor PIPEROOM_SECRET_KEY_PREVIOUS (the key changed,
	// or the blob is corrupt) → 409 secret_key_mismatch.
	ErrSecretKeyMismatch = errors.New("integrations: cannot open sealed secrets")
)

// SecretRefReason says why a secret:// reference cannot be read.
type SecretRefReason string

// Reasons a reference cannot be read, and their HTTP answers.
const (
	SecretRefInvalid         SecretRefReason = "invalid"          // 400 validation
	SecretRefUnknownProvider SecretRefReason = "unknown_provider" // 400 validation
	SecretRefDenied          SecretRefReason = "access_denied"    // 403 secret_access_denied
	SecretRefNotFound        SecretRefReason = "not_found"        // 404 not_found
	SecretRefUnreachable     SecretRefReason = "unreachable"      // 502 provider_unreachable
	SecretRefOther           SecretRefReason = "other"            // 400 validation
)

// SecretRefError: the reference (or listing prefix) in Field cannot be read.
// Err is the cause, for logs and errors.Is; it never holds a value.
type SecretRefError struct {
	Field  string
	Reason SecretRefReason
	Err    error
}

// Error implements error.
func (e *SecretRefError) Error() string {
	return fmt.Sprintf("integrations: field %s: secret reference cannot be read (%s): %v", e.Field, e.Reason, e.Err)
}

// Unwrap returns the cause.
func (e *SecretRefError) Unwrap() error { return e.Err }

// Dependent is a row that names a secret manager in a reference.
type Dependent struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Title string `json:"title"`
}

// InUseError: a secret manager that enabled rows reference cannot be
// deleted or disabled → 409 in_use with the dependents in the body.
type InUseError struct{ Dependents []Dependent }

// Error implements error.
func (e *InUseError) Error() string {
	return fmt.Sprintf("integrations: secret manager is used by %d integration(s)", len(e.Dependents))
}

// ValidationError is bad input on one field → 400 validation with "field".
type ValidationError struct {
	Field   string // field name; "" when not about one field
	Message string // safe to show; never contains a submitted value
}

// Error implements error.
func (e *ValidationError) Error() string {
	if e.Field == "" {
		return "integrations: " + e.Message
	}
	return fmt.Sprintf("integrations: field %s: %s", e.Field, e.Message)
}

// ChecksFailedError: a create or update whose test has a CheckFail → 422
// checks_failed, with the checks in the body. Nothing was saved.
type ChecksFailedError struct{ Checks []Check }

// Error implements error.
func (e *ChecksFailedError) Error() string { return "integrations: connection checks failed" }

// SecretOp says what an API body does to one secret field.
type SecretOp int

// Secret operations, from the JSON value of secrets.<field>:
// absent or "" → SecretKeep; "text" → SecretSet; {"$ref":"secret://…"} →
// SecretRef; null → SecretClear.
const (
	SecretKeep  SecretOp = iota // keep the stored value (create: leave unset)
	SecretSet                   // a typed-in value, sealed at rest
	SecretRef                   // a secret:// reference, stored in Config
	SecretClear                 // remove the stored value or reference
)

// SecretInput is one secret field of an API body.
type SecretInput struct {
	Op    SecretOp
	Value netsec.Secret // SecretSet only
	Ref   string        // SecretRef only; starts with RefPrefix
}

// Input is the body of POST /test, POST / and PUT /{id}.
type Input struct {
	Kind string // Kind.ID; on PUT it must equal the row's kind
	// ID, on POST /test only, names a saved row whose stored secrets fill the
	// SecretKeep fields, so an edit can be tested without retyping a password.
	ID string
	// Config holds non-secret fields as decoded from JSON (string, float64, bool).
	Config  map[string]any
	Secrets map[string]SecretInput
}

// Integration statuses shown on a card.
const (
	StatusConnected = "connected" // enabled and the last test had no fail
	StatusFailing   = "failing"   // enabled and the last test had a fail
	StatusDisabled  = "disabled"  // Enabled is false
	StatusUntested  = "untested"  // enabled, never tested (env rows before the first background test)
)

// View is an integration as the API returns it: no secret values.
type View struct {
	ID           string         `json:"id"`
	WorkspaceID  string         `json:"workspace_id"`
	Category     Category       `json:"category"`
	Kind         string         `json:"kind"`
	Title        string         `json:"title"`       // the kind's title
	Config       map[string]any `json:"config"`      // non-secret fields plus {"$ref": …} for referenced secret fields
	SecretsSet   []string       `json:"secrets_set"` // secret field names that have a value; [] when none
	Version      int64          `json:"version"`     // 0 for env rows
	Enabled      bool           `json:"enabled"`
	Status       string         `json:"status"`         // Status* consts
	LastTest     []Check        `json:"last_test"`      // [] when never tested
	LastTestedAt *time.Time     `json:"last_tested_at"` // null when never tested
	UpdatedBy    string         `json:"updated_by"`     // "env" for env rows
	UpdatedAt    time.Time      `json:"updated_at"`
	Source       string         `json:"source"` // SourceDB | SourceEnv
}

// CatalogKind is a Kind as GET /api/integrations/catalog returns it.
type CatalogKind struct {
	ID             string   `json:"id"`
	Category       Category `json:"category"`
	Title          string   `json:"title"`
	Summary        string   `json:"summary"`
	Logo           string   `json:"logo"`
	Reads          []string `json:"reads"`
	Never          []string `json:"never"`
	Fields         []Field  `json:"fields"`
	Required       bool     `json:"required"`
	HasSetupScript bool     `json:"has_setup_script"`
}

// ToCatalog converts a Kind for the catalog route. Nil slices become empty
// ones, so the JSON holds [] and never null.
func ToCatalog(k Kind) CatalogKind {
	c := CatalogKind{
		ID: k.ID, Category: k.Category, Title: k.Title, Summary: k.Summary, Logo: k.Logo,
		Reads: nonNil(k.Reads), Never: nonNil(k.Never), Fields: make([]Field, 0, len(k.Fields)),
		Required: k.Required, HasSetupScript: k.SetupScript != nil,
	}
	for _, f := range k.Fields {
		f.Options = nonNil(f.Options)
		c.Fields = append(c.Fields, f)
	}
	return c
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// AlertSetup is what an alert source's card shows. It never holds the secret;
// the rotate route adds it to its response once.
type AlertSetup struct {
	Source        string `json:"source"`         // alertmanager | grafana | pagerduty | datadog | generic
	IntegrationID string `json:"integration_id"` // "" until a secret was generated
	WebhookURL    string `json:"webhook_url"`    // <PIPEROOM_PUBLIC_URL>/api/alerts/<source>
	Snippet       string `json:"snippet"`        // copy-paste config for that tool, secret shown as <secret>
	SnippetLang   string `json:"snippet_lang"`   // yaml | json | text
	SecretSet     bool   `json:"secret_set"`     // a secret exists (from db or env)
	SecretSource  string `json:"secret_source"`  // SourceDB | SourceEnv | "" when none
}

// Service is the integrations logic behind /api/integrations (implemented by
// internal/integrations, called by internal/httpapi). The caller has already
// checked the admin rule; actor is the admin's user id. Every method is scoped
// to workspaceID.
type Service interface {
	// Catalog returns every kind in catalog order.
	Catalog() []Kind
	// SetupScript fills a kind's script from non-secret config. ErrUnknownKind;
	// a kind with no script returns "".
	SetupScript(kind string, config map[string]any) (string, error)
	// List returns db rows and env-sourced categories as views.
	List(ctx context.Context, workspaceID string) ([]View, error)
	// Test validates in, resolves references and runs the kind's checks.
	// Stores nothing (not even last_test). ErrRateLimited, ErrUnknownKind, *ValidationError.
	Test(ctx context.Context, workspaceID, actor string, in Input) ([]Check, error)
	// Create tests, seals, stores, applies the client, audits and notifies.
	// *ChecksFailedError, ErrDuplicate, ErrManagedByEnv, ErrSecretKeyMissing, *ValidationError.
	Create(ctx context.Context, workspaceID, actor string, in Input) (View, []Check, error)
	// Update is Create for an existing row, with optimistic locking.
	// Adds ErrNotFound and ErrVersionConflict.
	Update(ctx context.Context, workspaceID, actor, id string, ifVersion int64, in Input) (View, []Check, error)
	// SetEnabled enables (builds and applies) or disables (clears the client) a row.
	SetEnabled(ctx context.Context, workspaceID, actor, id string, enabled bool) (View, error)
	// Delete removes the row and clears its client.
	Delete(ctx context.Context, workspaceID, actor, id string) error
	// Audit returns the row's audit entries, newest first.
	Audit(ctx context.Context, workspaceID, id string, limit int) ([]AuditEntry, error)
	// AlertSetup returns the webhook URL and snippet. ErrUnknownAlertSource.
	AlertSetup(ctx context.Context, workspaceID, source string) (AlertSetup, error)
	// RotateAlertSecret generates and stores a new secret for source and
	// returns it once. ErrManagedByEnv when the env sets that source's secret.
	RotateAlertSecret(ctx context.Context, workspaceID, actor, source string) (AlertSetup, netsec.Secret, error)
	// SecretNames lists secret names under prefix in a secret_manager row
	// (at most 200). ErrNotSecretManager, ErrListUnsupported, ErrProviderUnreachable.
	SecretNames(ctx context.Context, workspaceID, id, prefix string) ([]string, error)
	// SecretKeys lists the field names of the JSON/key-value secret ref names
	// (its provider must be the row id's). Never values.
	SecretKeys(ctx context.Context, workspaceID, id, ref string) ([]string, error)
}

// Applier swaps live clients (internal/wiring's Live implements it). A failed
// Apply leaves the old client in place.
type Applier interface {
	// Apply builds rec's client with kind.Build from v and swaps it in.
	Apply(ctx context.Context, rec Integration, kind Kind, v Values) error
	// Clear swaps in "not configured" for rec's category (disable, delete).
	Clear(ctx context.Context, rec Integration) error
}

// Notifier tells open rooms (and Slack, when connected) that an integration
// changed. The room service implements it; it posts field names, never values.
type Notifier interface {
	IntegrationChanged(ctx context.Context, workspaceID string, e AuditEntry, kindTitle string)
}
