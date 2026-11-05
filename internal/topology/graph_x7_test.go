package topology

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// containsEdge reports whether edges has e, comparing the fields a test
// names. Edge holds a slice since X7 (Columns), so it is no longer
// comparable with == or slices.Contains.
func containsEdge(edges []Edge, e Edge) bool {
	for _, x := range edges {
		if x.From == e.From && x.To == e.To && x.Kind == e.Kind && x.Mapping == e.Mapping &&
			reflect.DeepEqual(x.Columns, e.Columns) && x.ColumnsOmitted == e.ColumnsOmitted {
			return true
		}
	}
	return false
}

// strictGraph decodes raw as a Graph, refusing unknown fields and trailing
// data, as a contract-shape check does.
func strictGraph(raw []byte) (Graph, error) {
	var g Graph
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err := dec.Decode(&g)
	return g, err
}

// TC1: a graph without Columns or Tags keeps its JSON bytes and its render
// after a round trip through the X7 types; the golden file test
// (TestRenderGolden) runs unchanged next to this.
func TestX7OldGraphUnchanged(t *testing.T) {
	b, g := goldenGraph(t)
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"tags"`)) || bytes.Contains(raw, []byte(`"columns`)) {
		t.Fatalf("a graph without X7 data grew X7 keys: %s", raw)
	}
	back, err := strictGraph(raw)
	if err != nil {
		t.Fatalf("strict decode: %v", err)
	}
	again, err := json.Marshal(back)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, again) {
		t.Fatalf("round trip changed the JSON\n got %s\nwant %s", again, raw)
	}
	if b.Render(back) != b.Render(g) {
		t.Fatal("render changed after a round trip")
	}
	// The pre-X7 edge and node shapes, byte for byte.
	e, _ := json.Marshal(Edge{From: "a", To: "b"})
	n, _ := json.Marshal(Node{ID: "pg:public.orders", Kind: KindSourceTable})
	if string(e) != `{"from":"a","to":"b"}` ||
		string(n) != `{"id":"pg:public.orders","kind":"source_table","name":"","layer":"","owner":"","health":""}` {
		t.Fatalf("pre-X7 shapes changed: %s %s", e, n)
	}
}

// TC2, E1: a graph with column lineage and tags survives a strict JSON round
// trip, zero values included, and an unknown field is refused.
func TestX7GraphRoundTrip(t *testing.T) {
	g := Graph{
		PipelineID: "orders",
		Nodes: []Node{
			{ID: "pg:public.orders", Kind: KindSourceTable, Name: "public.orders", Layer: "postgres", Owner: "priya",
				Health: HealthOK, Tags: map[string]string{TagPII: "customer_email:email", TagRetention: "7d"}},
			{ID: "topic:pg.public.orders", Kind: KindTopic, Name: "pg.public.orders", Health: HealthOK,
				Tags: map[string]string{TagRetention: RetentionInfinite}},
			{ID: "wh:analytics.orders", Kind: KindSinkTable, Health: HealthWarn,
				Tags: map[string]string{TagPII: "", TagPIIOmitted: "3"}},
			{ID: "connect:c", Kind: KindConnector},
		},
		Edges: []Edge{
			{From: "pg:public.orders", To: "connect:c"},
			{From: "connect:c", To: "topic:pg.public.orders", Columns: []ColumnMap{
				{Input: "pg:public.orders", From: "id", To: "after.id"},
				{Input: "pg:public.orders", From: "amount", To: "after.total", Transform: ColumnRename},
			}},
			{From: "group:g", To: "wh:analytics.orders", Mapping: MappingInferred, ColumnsOmitted: 2, Columns: []ColumnMap{
				{Input: "topic:pg.public.orders", From: "after.id", To: "id", Transform: ColumnUnwrap},
				{Input: "pg:public.orders", From: "id", To: "id", Mapping: MappingInferred},
				{To: "_loaded_at", Transform: ColumnAdded},
				{Input: "pg:public.orders", From: "discount", Transform: ColumnDropped},
				{},
			}},
		},
	}
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"tags":{"pii":"customer_email:email","retention":"7d"}`,
		`{"input":"pg:public.orders","from":"amount","to":"after.total","transform":"rename"}`,
		`{"to":"_loaded_at","transform":"added"}`,
		`{"input":"pg:public.orders","from":"discount","transform":"dropped"}`,
		`"columns_omitted":2`,
		`{}`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("JSON has no %s:\n%s", want, raw)
		}
	}
	back, err := strictGraph(raw)
	if err != nil {
		t.Fatalf("strict decode: %v", err)
	}
	if !reflect.DeepEqual(back, g) {
		t.Fatalf("round trip\n got %#v\nwant %#v", back, g)
	}
	for _, bad := range []string{
		`{"pipeline_id":"o","nodes":[],"edges":[{"from":"a","to":"b","columns":[{"from":"x","value":"secret"}]}]}`,
		`{"pipeline_id":"o","nodes":[{"id":"a","kind":"topic","name":"","layer":"","owner":"","health":"","labels":{}}],"edges":[]}`,
	} {
		if _, err := strictGraph([]byte(bad)); err == nil {
			t.Errorf("strict decode accepted an unknown field: %s", bad)
		}
	}
}

// Every node kind is either a dataset (column lineage ends there, an
// OpenLineage Dataset) or not; the split is fixed by the contract.
func TestX7IsDataset(t *testing.T) {
	want := map[NodeKind]bool{
		KindSourceTable: true, KindConnector: false, KindTopic: true, KindConsumerGroup: false, KindSinkTable: true,
		KindDLQTopic: true, KindSchemaSubject: false, KindDAG: false, KindTask: false, KindModel: true,
		KindKafkaEngine: true, KindMaterializedView: true, NodeKind("unknown"): false,
	}
	for k, w := range want {
		if IsDataset(k) != w {
			t.Errorf("IsDataset(%s) = %v, want %v", k, !w, w)
		}
	}
}

// E4: the column cap is enforced at the boundary.
func TestX7LimitColumns(t *testing.T) {
	mk := func(n int) []ColumnMap {
		out := make([]ColumnMap, n)
		for i := range out {
			out[i] = ColumnMap{Input: "pg:t", From: "c" + strconv.Itoa(i), To: "c" + strconv.Itoa(i)}
		}
		return out
	}
	for _, tt := range []struct{ n, kept, omitted int }{
		{0, 0, 0}, {1, 1, 0}, {MaxEdgeColumns - 1, MaxEdgeColumns - 1, 0}, {MaxEdgeColumns, MaxEdgeColumns, 0},
		{MaxEdgeColumns + 1, MaxEdgeColumns, 1}, {1600, MaxEdgeColumns, 600},
	} {
		in := mk(tt.n)
		kept, omitted := LimitColumns(in)
		if len(kept) != tt.kept || omitted != tt.omitted {
			t.Errorf("n=%d: kept %d omitted %d, want %d %d", tt.n, len(kept), omitted, tt.kept, tt.omitted)
		}
		if tt.n > 0 && kept[0].From != "c0" {
			t.Errorf("n=%d: the first maps were not kept", tt.n)
		}
		if tt.n > MaxEdgeColumns {
			// An append to the kept list must not write into the caller's tail.
			_ = append(kept, ColumnMap{From: "x"})
			if in[MaxEdgeColumns].From != "c"+strconv.Itoa(MaxEdgeColumns) {
				t.Errorf("n=%d: append to kept overwrote the input", tt.n)
			}
		}
	}
}

// E4, E6: tag keys and values are checked; a value never spans lines.
func TestX7ValidTag(t *testing.T) {
	for _, tt := range []struct {
		key, value string
		ok         bool
	}{
		{TagPII, "email:email", true},
		{TagRetention, "7d", true},
		{TagPIIOmitted, "", true},
		{"a_1", strings.Repeat("x", MaxTagValueBytes), true},
		{"a", strings.Repeat("x", MaxTagValueBytes+1), false},
		{"", "x", false},
		{"Pii", "x", false},
		{"1pii", "x", false},
		{"pii-tag", "x", false},
		{strings.Repeat("a", 33), "x", false},
		{strings.Repeat("a", 32), "x", true},
		{"pii", "a\nb", false},
		{"pii", "a\x00b", false},
		{"pii", "\xff", false},
		{"pii", "a\u202eb", false}, // bidi override (#358 rule)
		{"pii", "a\u2066b", false}, // bidi isolate
		{"pii", "e\u0301mail", true},
	} {
		if err := ValidTag(tt.key, tt.value); (err == nil) != tt.ok {
			t.Errorf("ValidTag(%q, %.20q) = %v, want ok=%v", tt.key, tt.value, err, tt.ok)
		}
	}
}

// The PII tag is "column:class" pairs sorted by column, read back exactly;
// what it cannot hold is counted, never silently lost.
func TestX7PIITag(t *testing.T) {
	v, omitted := FormatPIITag(map[string]PIIClass{
		"phone_number": PIIPhone, "customer_email": PIIEmail, "dob": PIIDOB,
		"a,b": PIIName, "x:y": PIIName, "bad\nname": PIIName, "": PIIName, "weird": PIIClass("hobby"),
		"liame_\u202etsuc": PIIEmail,
	})
	if v != "customer_email:email,dob:dob,phone_number:phone" || omitted != 6 {
		t.Fatalf("FormatPIITag = %q, %d", v, omitted)
	}
	back, err := ParsePIITag(v)
	if err != nil || len(back) != 3 || back["dob"] != PIIDOB || back["customer_email"] != PIIEmail {
		t.Fatalf("ParsePIITag = %v, %v", back, err)
	}
	if v, n := FormatPIITag(nil); v != "" || n != 0 {
		t.Fatalf("empty map = %q, %d", v, n)
	}
	if m, err := ParsePIITag(""); err != nil || len(m) != 0 {
		t.Fatalf("ParsePIITag(\"\") = %v, %v", m, err)
	}
	for _, bad := range []string{"email", "email:", ":email", "a:email,b", "a:hobby", "a:email,"} {
		if _, err := ParsePIITag(bad); err == nil {
			t.Errorf("ParsePIITag(%q) accepted", bad)
		}
	}

	// The cap: whole pairs only, the rest counted, and the value stays valid.
	many := map[string]PIIClass{}
	for i := range 400 {
		many["customer_email_"+strconv.Itoa(1000+i)] = PIIEmail
	}
	v, omitted = FormatPIITag(many)
	if len(v) > MaxTagValueBytes || omitted == 0 {
		t.Fatalf("cap: %d bytes, %d omitted", len(v), omitted)
	}
	back, err = ParsePIITag(v)
	if err != nil || len(back)+omitted != 400 {
		t.Fatalf("cap: %d read back + %d omitted != 400 (%v)", len(back), omitted, err)
	}
	if err := ValidTag(TagPII, v); err != nil {
		t.Fatalf("a formatted tag is not a valid tag: %v", err)
	}
	// The kept pairs are the first columns in name order: nothing after a cut.
	for i := range len(back) {
		if _, ok := back["customer_email_"+strconv.Itoa(1000+i)]; !ok {
			t.Fatalf("cap: column %d missing while later ones were kept", i)
		}
	}
	// A short name after a long one that did not fit is cut too.
	v, omitted = FormatPIITag(map[string]PIIClass{"a": PIIEmail, "b" + strings.Repeat("x", MaxTagValueBytes): PIIEmail, "c": PIIEmail})
	if v != "a:email" || omitted != 2 {
		t.Fatalf("prefix cut = %q, %d", v, omitted)
	}
	if len(PIIClasses()) != 9 {
		t.Fatalf("PIIClasses = %v", PIIClasses())
	}
}

// Declared retention parses and formats in one form, both ways.
func TestX7Retention(t *testing.T) {
	day := 24 * time.Hour
	for _, tt := range []struct {
		in       string
		d        time.Duration
		infinite bool
		ok       bool
	}{
		{"7d", 7 * day, false, true},
		{"36h", 36 * time.Hour, false, true},
		{"90m", 90 * time.Minute, false, true},
		{"45s", 45 * time.Second, false, true},
		{"infinite", 0, true, true},
		{"36500d", 36500 * day, false, true},
		{"36501d", 0, false, false},
		{"0d", 0, false, false},
		{"-1d", 0, false, false},
		{"+1d", 0, false, false},
		{"1.5d", 0, false, false},
		{"7", 0, false, false},
		{"d", 0, false, false},
		{"", 0, false, false},
		{"7w", 0, false, false},
		{"7D", 0, false, false},
		{"Infinite", 0, false, false},
		{"99999999999999999999d", 0, false, false},
	} {
		d, inf, err := ParseRetention(tt.in)
		if (err == nil) != tt.ok || d != tt.d || inf != tt.infinite {
			t.Errorf("ParseRetention(%q) = %v, %v, %v", tt.in, d, inf, err)
		}
	}
	for _, tt := range []struct {
		d        time.Duration
		infinite bool
		want     string
	}{
		{7 * day, false, "7d"},
		{day, false, "1d"},
		{36 * time.Hour, false, "36h"},
		{90 * time.Minute, false, "90m"},
		{45*time.Second + 500*time.Millisecond, false, "45s"},
		{500 * time.Millisecond, false, "0s"},
		{0, false, "0s"},
		{-time.Hour, false, "0s"},
		{0, true, "infinite"},
		{MaxRetention + time.Hour, false, "infinite"},
	} {
		got := FormatRetention(tt.d, tt.infinite)
		if got != tt.want {
			t.Errorf("FormatRetention(%v, %v) = %q, want %q", tt.d, tt.infinite, got, tt.want)
		}
		if got == "0s" {
			continue
		}
		d, inf, err := ParseRetention(got)
		if err != nil || inf != (got == RetentionInfinite) || (!inf && d != tt.d.Truncate(time.Second)) {
			t.Errorf("ParseRetention(FormatRetention(%v)) = %v, %v, %v", tt.d, d, inf, err)
		}
	}
}
