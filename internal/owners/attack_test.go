package owners_test

import (
	"os"
	"path/filepath"
	"testing"

	"piperoom/internal/owners"
)

// Security audit 2026-10 (reviews/security-audit-2026-10.md): owners.yaml
// edge cases an attacker (or a tired admin) could use to widen who may
// approve. A passing test is a defended attack.

func loadOwners(t *testing.T, doc string) (*owners.Owners, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return owners.Load(p)
}

const pipelineHead = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
`

// A-OWN-1 (fixed), and the owners.yaml spellings that were already refused.
// `owner:` for `owners:` used to be dropped silently, so kafka-connect fell
// back to the top-level owner (arjun) although the file meant to limit it to
// the on-call group.
func TestAttack_OwnersYAMLEdgeCases(t *testing.T) {
	bad := map[string]string{
		"pipeline owner: typo (fail-open)": pipelineHead + `    owner:
      kafka-connect: { approvers: [group:dataeng-oncall] }
layers:
  kafka-connect: arjun
`,
		"top-level auditor: typo": pipelineHead + "auditor: [user:meera]\n",
		"rights backups: typo": pipelineHead + `    owners:
      kafka-connect: { approvers: [group:x], backups: [user:priya] }
`,
		"duplicate layer key":      "layers:\n  kafka: arjun\n  kafka: priya\n",
		"duplicate pipeline id":    pipelineHead + "  - {id: orders, topic_prefix: pg, source_tables: [a], connectors: [b]}\n",
		"duplicate owners layer":   pipelineHead + "    owners:\n      kafka: {approvers: [user:a]}\n      kafka: {approvers: [user:b]}\n",
		"bad prefix case":          "layers:\n  kafka: User:arjun\n",
		"unknown prefix":           "layers:\n  kafka: grp:ops\n",
		"empty user":               "layers:\n  kafka: \"user:\"\n",
		"empty group":              "layers:\n  kafka: \"group: \"\n",
		"empty string":             "layers:\n  kafka: \"\"\n",
		"null item":                "layers:\n  kafka: [user:arjun, null]\n",
		"empty list":               "layers:\n  kafka: []\n",
		"user id with a space":     "layers:\n  kafka: \"user:ar jun\"\n",
		"comma in a name":          "layers:\n  kafka: \"group:a,b\"\n",
		"map instead of principal": "layers:\n  kafka: {user: arjun}\n",
		"unknown layer":            "layers:\n  kafak: arjun\n",
		"owners with no approvers": pipelineHead + "    owners:\n      kafka: {backup: [user:priya]}\n",
		"owners for unknown layer": pipelineHead + "    owners:\n      kafak: {approvers: [user:priya]}\n",
	}
	for name, doc := range bad {
		if _, err := loadOwners(t, doc); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A-OWN-2. Group and user matching is exact: a look-alike claim value, a
// group named like a user principal, a missing claim, or a pipeline id that
// is not configured grants nothing.
func TestAttack_PrincipalMatchingIsExact(t *testing.T) {
	o, err := loadOwners(t, pipelineHead+`    owners:
      kafka-connect: { approvers: [group:dataeng-oncall], backup: [user:meera] }
layers:
  kafka-connect: arjun
  postgres: priya
auditors: [group:security]
`)
	if err != nil {
		t.Fatal(err)
	}
	for _, groups := range [][]string{
		nil, {}, {""}, {"Dataeng-Oncall"}, {"dataeng-oncall "}, {" dataeng-oncall"},
		{"dataeng\u2011oncall"}, {"dataeng-oncall\u200b"}, {"group:dataeng-oncall"}, {"dataeng-oncall,security"},
	} {
		if g, ok := o.CanApprove("orders", "kafka-connect", "mallory", groups); ok {
			t.Errorf("groups %q approve kafka-connect (%s)", groups, g)
		}
	}
	// A group claim spelled like a user principal does not make the user.
	if _, ok := o.CanApprove("orders", "postgres", "mallory", []string{"user:priya", "priya"}); ok {
		t.Error("a group named user:priya approved priya's layer")
	}
	// A user id spelled like the principal does not match either.
	for _, id := range []string{"user:priya", "Priya", "priya ", "pr\u0456ya", ""} {
		if _, ok := o.CanApprove("orders", "postgres", id, nil); ok {
			t.Errorf("user %q approved priya's layer", id)
		}
	}
	// The pipeline block replaces the top-level owner: arjun is out there.
	if _, ok := o.CanApprove("orders", "kafka-connect", "arjun", nil); ok {
		t.Error("the top-level owner approved a layer the pipeline replaces")
	}
	// An unknown pipeline fails closed.
	if _, ok := o.CanApprove("renamed", "postgres", "priya", nil); ok {
		t.Error("an unknown pipeline fell back to the default owners")
	}
	// auditors: never makes a member or an approver.
	if o.IsMemberPrincipal("mallory", []string{"security"}) {
		t.Error("an auditor is a member")
	}
	if l := o.LayersFor("mallory", []string{"security"}); len(l) != 0 {
		t.Errorf("an auditor approves %v", l)
	}
}

// The shipped owners files still load with the stricter parser.
func TestAttack_ShippedOwnersFilesStillLoad(t *testing.T) {
	for _, p := range []string{"../../dev/owners.yaml", "../../deploy/config/owners.example.yaml", "../discover/testdata/draft.golden.yaml"} {
		if _, err := owners.Load(p); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}
