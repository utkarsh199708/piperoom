package topology

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
)

// piiOwners is lineageOwners loaded from YAML with a pii: block, so the
// overrides are parsed and validated as in production.
func piiOwners(t *testing.T, piiBlock string) *owners.Owners {
	t.Helper()
	y := `pipelines:
  - id: orders
    title: Orders CDC
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector, orders-sink]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
` + piiBlock + `layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func nodeOf(t *testing.T, g Graph, id string) Node {
	t.Helper()
	for _, n := range g.Nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("no node %s", id)
	return Node{}
}

var (
	piiSrcCols  = []string{"id", "customer_email", "phone_number", "phone_verified", "emailed_at", "notes", "updated_at"}
	piiSinkCols = []string{"id", "customer_email", "card_last4", "ip_count", "updated_at"}
	piiTypes    = map[string]string{
		"id": "bigint", "customer_email": "character varying", "phone_number": "text", "phone_verified": "boolean",
		"emailed_at": "timestamp with time zone", "notes": "text", "updated_at": "timestamp with time zone",
		"card_last4": "smallint", "ip_count": "integer",
	}
)

func piiFixture() *lineageFixture {
	f := newLineageFixture(piiSrcCols, piiSinkCols)
	f.src.types, f.sink.types = piiTypes, piiTypes
	return f
}

// E1: source and sink table nodes carry the classified columns as sorted
// column:class pairs; other nodes and non-PII columns get nothing; the map
// render and the JSON carry the tag.
func TestPIITagsOnTableNodes(t *testing.T) {
	g := piiFixture().build(t)
	if got := nodeOf(t, g, srcID).Tags; !maps.Equal(got, map[string]string{TagPII: "customer_email:email,phone_number:phone"}) {
		t.Fatalf("source tags = %v", got)
	}
	if got := nodeOf(t, g, sinkID).Tags; !maps.Equal(got, map[string]string{TagPII: "card_last4:card,customer_email:email"}) {
		t.Fatalf("sink tags = %v", got)
	}
	for _, n := range g.Nodes {
		if n.ID != srcID && n.ID != sinkID && n.Tags != nil {
			t.Fatalf("node %s has tags %v", n.ID, n.Tags)
		}
	}
	b := piiFixture().builder()
	if r := b.Render(g); !strings.Contains(r, "tag.pii=customer_email:email,phone_number:phone") {
		t.Fatalf("render has no pii tag:\n%s", r)
	}
	raw, _ := json.Marshal(g)
	if !bytes.Contains(raw, []byte(`"tags":{"pii":"card_last4:card,customer_email:email"}`)) {
		t.Fatalf("JSON: %s", raw)
	}
	for _, n := range g.Nodes {
		for k, v := range n.Tags {
			if err := ValidTag(k, v); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TC2: owners.yaml pii include adds (with a class or other), exclude removes,
// per table or for every table.
func TestPIITagsOwnersOverrides(t *testing.T) {
	f := piiFixture()
	b := f.builder()
	b.o = piiOwners(t, `    pii:
      include: [public.orders.notes, "analytics.orders.ip_count:ip"]
      exclude: [public.orders.phone_number, customer_email]
`)
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if got := nodeOf(t, g, srcID).Tags[TagPII]; got != "notes:other" {
		t.Fatalf("source pii = %q", got)
	}
	if got := nodeOf(t, g, sinkID).Tags[TagPII]; got != "card_last4:card,ip_count:ip" {
		t.Fatalf("sink pii = %q", got)
	}
	// Excluding every hit leaves no tag at all.
	b.o = piiOwners(t, "    pii:\n      exclude: [customer_email, phone_number, card_last4]\n")
	g, err = b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if n := nodeOf(t, g, srcID); n.Tags != nil {
		t.Fatalf("all excluded: tags %v", n.Tags)
	}
}

// rowWatch wraps the source fake and records every row read (TC3).
type rowWatch struct {
	colSource
	mu    *sync.Mutex
	reads *[]string
}

func (r rowWatch) Row(ctx context.Context, table, col, key string) (map[string]any, bool, error) {
	r.mu.Lock()
	*r.reads = append(*r.reads, table)
	r.mu.Unlock()
	return r.colSource.Row(ctx, table, col, key)
}

// TC3, E6: tagging reads the catalog only (one Columns call per table, the
// pipeline's own) and never a row; a value planted in the fake cannot reach
// the graph. The SQL of the catalog read is pinned in pgconv
// (TestColumnsSQLCatalogOnly).
func TestPIITagsCatalogOnly(t *testing.T) {
	f := piiFixture()
	b := f.builder()
	var mu sync.Mutex
	var reads []string
	cs := b.src.(colSource)
	cs.rows = map[string]map[string]any{"public.orders|7": {"customer_email": "jane@example.com"}}
	b.src = rowWatch{cs, &mu, &reads}
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(reads) != 0 {
		t.Fatalf("rows read: %v", reads)
	}
	if !slices.Equal(f.src.asked, []string{"public.orders"}) || !slices.Equal(f.sink.asked, []string{"analytics.orders"}) ||
		len(f.src.refused)+len(f.sink.refused) != 0 {
		t.Fatalf("catalog calls: src %v/%v sink %v/%v", f.src.asked, f.src.refused, f.sink.asked, f.sink.refused)
	}
	raw, _ := json.Marshal(g)
	if bytes.Contains(raw, []byte("jane@")) || strings.Contains(b.Render(g), "jane@") {
		t.Fatal("a row value reached the graph")
	}
}

// E5: a catalog that fails or is not supported leaves its table untagged and
// never fails the build; the other side is still tagged.
func TestPIITagsReaderFailures(t *testing.T) {
	for name, err := range map[string]error{
		"unsupported": fmt.Errorf("snowflake: %w", clients.ErrNotSupported),
		"down":        errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			f := piiFixture()
			f.sink.err = err
			g := f.build(t)
			if n := nodeOf(t, g, sinkID); n.Tags != nil {
				t.Fatalf("sink tags %v", n.Tags)
			}
			if nodeOf(t, g, srcID).Tags[TagPII] == "" {
				t.Fatal("source lost its tag")
			}
		})
	}
	// No catalog reader at all: no tags, the graph as before X7.
	f := piiFixture()
	b := f.builder()
	b.src = b.src.(colSource).fakeSource
	b.wh = b.wh.(colWarehouse).fakeWarehouse
	g, err := b.Build(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range g.Nodes {
		if n.Tags != nil {
			t.Fatalf("node %s tagged without a catalog", n.ID)
		}
	}
}

// E4: a wide table of PII columns gives a tag within MaxTagValueBytes, the
// rest counted in pii_omitted; a name the tag cannot hold is counted too.
func TestPIITagsCap(t *testing.T) {
	var cols []string
	for i := range clients.MaxTableColumns {
		cols = append(cols, fmt.Sprintf("customer_email_%04d", i))
	}
	f := newLineageFixture(append(cols, "id", "updated_at"), fourCols)
	g := f.build(t)
	n := nodeOf(t, g, srcID)
	v, om := n.Tags[TagPII], n.Tags[TagPIIOmitted]
	pairs, err := ParsePIITag(v)
	if err != nil || len(v) > MaxTagValueBytes || len(pairs) == 0 {
		t.Fatalf("pii tag %d bytes, %d pairs, %v", len(v), len(pairs), err)
	}
	if got, _ := strconv.Atoi(om); got != clients.MaxTableColumns-len(pairs) {
		t.Fatalf("pii_omitted = %q, want %d", om, clients.MaxTableColumns-len(pairs))
	}
	if !strings.HasPrefix(v, "customer_email_0000:email,customer_email_0001:email") {
		t.Fatalf("tag is not sorted from the first column: %.80s", v)
	}
}

// The classifier's classes and topology.PIIClasses agree: every class it
// answers is a PIIClass, and PIIClasses is those plus owners' "other".
func TestPIIClassesMatchClassifier(t *testing.T) {
	want := []PIIClass{}
	for _, c := range redact.ColumnClasses() {
		want = append(want, PIIClass(c))
	}
	want = append(want, PIIClass(owners.PIIClassOther))
	if !slices.Equal(PIIClasses(), want) {
		t.Fatalf("PIIClasses %v, classifier + other %v", PIIClasses(), want)
	}
}
