package room

import (
	"slices"
	"testing"
	"time"
)

// X4c: the agent's Alert carries the teams of the room pipeline's tenant
// (ids and owned services), never another tenant's.
func TestNewAlertFillsTeams(t *testing.T) {
	own := loadOwnersFile(t, `
tenants:
  a: {members: [user:ann]}
  b: {members: [user:ben]}
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [checkout-api], tenant: a}
  - {id: billing, slack_channel: C0456EFGH, owns: [billing-api], tenant: b}
pipelines:
  - id: orders
    tenant: a
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
`)
	svc := kindService(t, own, "", nil)
	got := svc.newAlert("r", "t", "orders", time.Time{}, nil).Teams
	if len(got) != 1 || got[0].ID != "checkout" || !slices.Equal(got[0].Owns, []string{"checkout-api"}) {
		t.Fatalf("Teams = %+v, want only tenant a's checkout team", got)
	}
	if got := svc.newAlert("r", "t", "ghost", time.Time{}, nil).Teams; got != nil {
		t.Fatalf("unknown pipeline: Teams = %+v, want nil", got)
	}
	if got := kindService(t, nil, "", nil).newAlert("r", "t", "orders", time.Time{}, nil).Teams; got != nil {
		t.Fatalf("no owners: Teams = %+v, want nil", got)
	}
}
