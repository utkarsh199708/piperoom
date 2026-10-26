package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// objectOwnersReader is implemented by source clients that read who owns
// tables and a publication (internal/clients/postgres). It is optional and
// not on the clients contract: a client without it (MySQL, older fakes) gets
// a not-supported answer, never a crash.
type objectOwnersReader interface {
	ObjectOwners(ctx context.Context, role, publication string, tables []string) (postgres.ObjectOwners, error)
}

// The verdicts pg_object_owners gives each table and the publication.
const (
	verdictOwner     = "connector_is_owner"
	verdictSuperuser = "connector_is_superuser"
	verdictMember    = "connector_is_member_of_owner"
	verdictDiffers   = "owner_differs"
	verdictMissing   = "missing"
	objectOwnersTool = "pg_object_owners"
	ownershipAdvice  = "Debezium's role must own the publication and its tables, or be an inheriting member of their owner role, to create or alter the publication (ALTER PUBLICATION ... ADD/SET TABLE); otherwise its task fails with \"must be owner of table ...\" or \"must be owner of publication ...\"\n"
)

// tableOwnerView is one pipeline source table in pg_object_owners' Data.
type tableOwnerView struct {
	Table                    string `json:"table"`
	Exists                   bool   `json:"exists"`
	Owner                    string `json:"owner,omitempty"`
	ConnectorIsOwner         bool   `json:"connector_is_owner"`
	ConnectorIsMemberOfOwner bool   `json:"connector_is_member_of_owner"`
	InPublication            bool   `json:"in_publication"`
	Verdict                  string `json:"verdict"`
}

// publicationOwnerView is the connector's publication in pg_object_owners'
// Data. PublishedTables lists only the pipeline's own tables.
type publicationOwnerView struct {
	Name                     string   `json:"name"`
	NameSource               string   `json:"name_source"`
	Exists                   bool     `json:"exists"`
	Owner                    string   `json:"owner,omitempty"`
	AllTables                bool     `json:"all_tables"`
	ConnectorIsOwner         bool     `json:"connector_is_owner"`
	ConnectorIsMemberOfOwner bool     `json:"connector_is_member_of_owner"`
	PublishedTables          []string `json:"published_tables"`
	Verdict                  string   `json:"verdict"`
}

// objectOwnersData is pg_object_owners' Data: bounded by the pipeline's
// table count (pipelineTables caps it). No password, no session or query.
type objectOwnersData struct {
	Connector           string                `json:"connector"`
	ConnectorUser       string                `json:"connector_user"`
	ConnectorUserSource string                `json:"connector_user_source"`
	RoleExists          bool                  `json:"role_exists"`
	Superuser           bool                  `json:"superuser"`
	Replication         bool                  `json:"replication"`
	Publication         *publicationOwnerView `json:"publication,omitempty"`
	Tables              []tableOwnerView      `json:"tables,omitempty"`
	// OwnerDiffers counts the objects (tables and publication) with verdict
	// owner_differs.
	OwnerDiffers int `json:"owner_differs"`
	// NotSupported is true when the source client cannot read owners.
	NotSupported bool `json:"not_supported,omitempty"`
}

// ownerVerdict decides one object's verdict. A superuser passes every
// ownership check, so it is not owner_differs even when it owns nothing.
func ownerVerdict(exists, roleExists, superuser, isOwner, hasOwnerPrivs bool) string {
	switch {
	case !exists:
		return verdictMissing
	case !roleExists:
		return verdictDiffers
	case isOwner:
		return verdictOwner
	case superuser:
		return verdictSuperuser
	case hasOwnerPrivs:
		return verdictMember
	}
	return verdictDiffers
}

// RegisterObjectOwners registers pg_object_owners for a Postgres source; a
// MySQL source gets nothing (it has no publication). Register leaves the tool
// out so the existing tool lists stay as they are until wiring adds it.
func RegisterObjectOwners(reg *tools.Registry, d Deps) error {
	if d.Owners == nil {
		return fmt.Errorf("dbtools: Owners is nil (wiring error)")
	}
	if d.SourceKind == owners.SourceKindMySQL {
		return nil
	}
	if err := reg.Register(pgObjectOwnersTool(d)); err != nil {
		return fmt.Errorf("dbtools: register %s: %w", objectOwnersTool, err)
	}
	return nil
}

// pgObjectOwnersTool reports who owns the pipeline's source tables and the
// connector's publication, against the connector's database.user.
func pgObjectOwnersTool(d Deps) tools.Tool {
	return &tool{
		name: objectOwnersTool,
		desc: "For the pipeline's Debezium connector, show who owns each source table and the connector's publication (publication.name, default dbz_publication), whether the publication is FOR ALL TABLES and which pipeline tables it publishes, and whether the connector role (database.user) owns them, is an inheriting member of the owner role, is superuser, and has REPLICATION. Each object gets a verdict: connector_is_owner, connector_is_superuser, connector_is_member_of_owner, owner_differs (the role cannot alter it) or missing. Call it when a connector task fails with an ownership or permission error (\"must be owner of table\", \"must be owner of publication\", permission denied) or a publication error, before ruling out roles or the publication; source_role_grants checks only login, REPLICATION and SELECT.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			// Validated before any I/O: a table name that is not
			// schema.table identifiers never reaches the client.
			tables, err := pipelineTables(p.ID, p.SourceTables)
			if err != nil {
				return tools.Result{}, err
			}
			ctx, cancel := toolCtx(ctx)
			defer cancel()

			// Role and publication come from the connector config, never
			// from model input: no probe of arbitrary roles.
			conn, err := resolveConnector(ctx, objectOwnersTool, p, d.Connect)
			if err != nil {
				return tools.Result{}, err
			}
			data := objectOwnersData{
				Connector:           conn.name,
				ConnectorUser:       conn.role,
				ConnectorUserSource: "connector " + conn.name + " config database.user",
			}
			r, ok := d.Src.(objectOwnersReader)
			if !ok {
				return notSupportedOwners(data), nil
			}
			oo, err := r.ObjectOwners(ctx, conn.role, conn.publication, tables)
			if errors.Is(err, clients.ErrNotSupported) {
				return notSupportedOwners(data), nil
			}
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: %s: %w", objectOwnersTool, err)
			}
			fillObjectOwners(&data, conn, oo)
			summary, truncated := tools.Truncate(objectOwnersSummary(data))
			return tools.Result{Summary: summary, Data: data, Truncated: truncated || len(data.Tables) > maxReplicationLines}, nil
		},
	}
}

// notSupportedOwners is the answer for a source client that cannot read owners.
func notSupportedOwners(data objectOwnersData) tools.Result {
	data.NotSupported = true
	return tools.Result{
		Summary: "object owners not checked: the source client cannot read table and publication owners (not supported); ownership is unknown, not ruled out\n",
		Data:    data,
	}
}

// fillObjectOwners maps the client read onto Data with a verdict per object.
// Owner names are catalog text: folded to one short line.
func fillObjectOwners(data *objectOwnersData, conn connectorConfig, oo postgres.ObjectOwners) {
	data.RoleExists, data.Superuser, data.Replication = oo.RoleExists, oo.RoleExists && oo.Superuser, oo.RoleExists && oo.Replication
	pub := publicationOwnerView{
		Name: conn.publication, NameSource: conn.publicationSource, Exists: oo.Publication.Exists,
		Owner: oneLineShort(oo.Publication.Owner, 64), AllTables: oo.Publication.AllTables,
		ConnectorIsOwner:         oo.RoleExists && oo.Publication.RoleIsOwner,
		ConnectorIsMemberOfOwner: oo.RoleExists && oo.Publication.RoleHasOwnerPrivs && !oo.Publication.RoleIsOwner,
		PublishedTables:          []string{},
	}
	if pub.NameSource == "" {
		pub.NameSource = "connector " + conn.name + " config publication.name"
	}
	pub.Verdict = ownerVerdict(pub.Exists, oo.RoleExists, oo.Superuser, pub.ConnectorIsOwner, oo.Publication.RoleHasOwnerPrivs)
	if pub.Verdict == verdictDiffers {
		data.OwnerDiffers++
	}
	for _, t := range oo.Tables {
		v := tableOwnerView{
			Table: t.Table, Exists: t.Exists, Owner: oneLineShort(t.Owner, 64),
			ConnectorIsOwner:         oo.RoleExists && t.RoleIsOwner,
			ConnectorIsMemberOfOwner: oo.RoleExists && t.RoleHasOwnerPrivs && !t.RoleIsOwner,
			InPublication:            pub.Exists && t.InPublication,
		}
		v.Verdict = ownerVerdict(t.Exists, oo.RoleExists, oo.Superuser, v.ConnectorIsOwner, t.RoleHasOwnerPrivs)
		if v.Verdict == verdictDiffers {
			data.OwnerDiffers++
		}
		if v.InPublication {
			pub.PublishedTables = append(pub.PublishedTables, t.Table)
		}
		data.Tables = append(data.Tables, v)
	}
	data.Publication = &pub
}

// objectOwnersSummary leads with the owner_differs lines so truncation never
// cuts the finding, then the role, the publication and one line per table.
func objectOwnersSummary(d objectOwnersData) string {
	var sb strings.Builder
	role := oneLineShort(d.ConnectorUser, 64)
	pub := d.Publication
	if pub.Verdict == verdictDiffers {
		fmt.Fprintf(&sb, "owner_differs: publication %s is owned by %s, not by %s\n", oneLineShort(pub.Name, 64), orDash(pub.Owner), role)
	}
	for _, t := range d.Tables {
		if t.Verdict == verdictDiffers {
			fmt.Fprintf(&sb, "owner_differs: %s is owned by %s, not by %s\n", t.Table, orDash(t.Owner), role)
		}
	}
	if d.OwnerDiffers > 0 {
		sb.WriteString(ownershipAdvice)
	}

	fmt.Fprintf(&sb, "connector %s role %s: ", d.Connector, role)
	if !d.RoleExists {
		sb.WriteString("the role does not exist (connector config database.user)\n")
	} else {
		fmt.Fprintf(&sb, "exists=yes superuser=%s replication=%s\n", yesNo(d.Superuser), yesNo(d.Replication))
	}

	name := oneLineShort(pub.Name, 64)
	if pub.NameSource == "default" {
		name += " (Debezium default name)"
	}
	if !pub.Exists {
		fmt.Fprintf(&sb, "publication %s: does not exist, verdict=%s\n", name, verdictMissing)
	} else {
		fmt.Fprintf(&sb, "publication %s: owner=%s all_tables=%s publishes %d of %d pipeline tables, verdict=%s\n",
			name, orDash(pub.Owner), yesNo(pub.AllTables), len(pub.PublishedTables), len(d.Tables), pub.Verdict)
	}

	shown := min(len(d.Tables), maxReplicationLines)
	for _, t := range d.Tables[:shown] {
		if !t.Exists {
			fmt.Fprintf(&sb, "%s: does not exist, verdict=%s\n", t.Table, verdictMissing)
			continue
		}
		fmt.Fprintf(&sb, "%s: owner=%s in_publication=%s, verdict=%s\n", t.Table, orDash(t.Owner), yesNo(t.InPublication), t.Verdict)
	}
	if more := len(d.Tables) - shown; more > 0 {
		fmt.Fprintf(&sb, "(%d more %s, in Data)\n", more, tools.Plural(more, "table", "tables"))
	}
	return sb.String()
}
