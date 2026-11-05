//go:build xwavegen

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/topology"
)

// ---- scenarios ----

type scen struct {
	file  string
	build func() *xstate
	st    *xstate
	// narrow: a second reconcile answer (a sequence) over this window.
	narrow []time.Time
	calls  []call
	// watcher window on the fixed clock
	watchFrom, watchUntil time.Time
}

func fx(h, m, s, ms int) time.Time {
	return time.Date(2026, 10, 22, h, m, s, ms*int(time.Millisecond), time.UTC)
}

func baseState(rng *rand.Rand, until time.Time) *xstate {
	s := newState()
	s.history(1300, time.Date(2026, 10, 21, 7, 12, 5, 0, time.UTC), until, rng)
	return s
}

func noop(*orow) {}

// ---- base world on a clock ----

func buildWorld(st *xstate, c clock) *xw {
	w := newWorld()
	cfg := map[string]string{
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "postgres",
		"database.port": "5432", "database.user": "piperoom", "database.password": "piperoom-dev", "database.dbname": "orders",
		"topic.prefix": "pg", "plugin.name": "pgoutput", "slot.name": "debezium_orders",
		"publication.autocreate.mode": "filtered", "table.include.list": "public.orders,public.devctl_signal", "tasks.max": "1",
		"signal.data.collection": "public.devctl_signal",
	}
	w.addConnector("pg-orders-connector", "source", "RUNNING", "connect:8083", cfg, task(0, "RUNNING", "connect:8083", ""))
	w.subjects["pg.public.orders-key"] = []schema{{1, keySchema(ordersTopic, avroField("id", avroLong))}}
	w.subjects["pg.public.orders-value"] = []schema{{2, envelope(ordersTopic, avroField("id", avroLong), avroField("customer_id", avroLong),
		avroField("amount", avroInt), avroField("status", avroString), avroField("updated_at", avroZoned))}}
	w.slots = []clients.ReplicationSlot{slot("debezium_orders", true, 1146880, "reserved")}
	w.pubs = []postgres.PublicationTable{{Schema: "public", Table: "orders", InPublication: true, Selectable: true}}
	x := &xw{world: w, st: st, c: c}
	w.topics[ordersTopic] = xkafka{fakeKafka{w}, x}.offsets()
	return x
}

func clientsOf(x *xw) (xsrc, xwh, xkafka) {
	return xsrc{fakeSource{x.world}, x}, xwh{fakeWarehouse{x.world}, x}, xkafka{fakeKafka{x.world}, x}
}

func runTool(o *owners.Owners, x *xw, name string, input map[string]any, c clock) outObs {
	conn := fakeConnect{x.world}
	src, wh, k := clientsOf(x)
	reg := tools.NewRegistry()
	if err := kafkatools.Register(reg, kafkatools.Deps{Connect: conn, Registry: fakeRegistry{x.world}, Kafka: k, Owners: o}); err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	b := topology.NewBuilder(o, conn, k, src, wh)
	if err := dbtools.Register(reg, dbtools.Deps{Src: src, Wh: wh, Builder: b, Connect: conn, Owners: o, Kafka: k}); err != nil {
		return outObs{Error: "gen: " + err.Error()}
	}
	t, ok := reg.Get(name)
	if !ok {
		return outObs{Error: "gen: unknown tool " + name}
	}
	raw, _ := json.Marshal(input)
	res, err := t.Run(tools.WithPipelineID(context.Background(), "orders"), raw)
	fix := func(s string) string { return shiftTimes(s, c) }
	if err != nil {
		return outObs{Error: fix(err.Error())}
	}
	ob := outObs{Summary: fix(res.Summary), Truncated: res.Truncated}
	if res.Data != nil {
		b, _ := json.Marshal(res.Data)
		ob.Data = decodeData([]byte(fix(string(b))))
	}
	return ob
}

var timestampRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`)

func shiftTimes(s string, c clock) string {
	shift := fixedNow.Sub(c.now)
	return timestampRe.ReplaceAllStringFunc(s, func(m string) string {
		t, err := time.Parse(time.RFC3339Nano, m)
		if err != nil {
			return m
		}
		if d := t.Sub(c.now); d > 60*24*time.Hour || d < -60*24*time.Hour {
			return m
		}
		layout := time.RFC3339
		if strings.Contains(m, ".") {
			layout = time.RFC3339Nano
		}
		return t.Add(shift).UTC().Format(layout)
	})
}

func decodeData(b []byte) any {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return string(b)
	}
	return plain(v)
}

func plain(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = plain(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = plain(e)
		}
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	}
	return v
}

type outRec struct {
	Tool      string         `yaml:"tool"`
	Input     map[string]any `yaml:"input,omitempty,flow"`
	Summary   string         `yaml:"summary,omitempty"`
	Data      any            `yaml:"data,omitempty"`
	Truncated bool           `yaml:"truncated,omitempty"`
	Error     string         `yaml:"error,omitempty"`
	Sequence  []outObs       `yaml:"sequence,omitempty"`
}

type outObs struct {
	Summary   string `yaml:"summary,omitempty"`
	Data      any    `yaml:"data,omitempty"`
	Truncated bool   `yaml:"truncated,omitempty"`
	Error     string `yaml:"error,omitempty"`
}

func (s scen) recordings(c clock, o *owners.Owners) ([]outRec, *xw, error) {
	x := buildWorld(s.st, c)
	var out []outRec
	for _, cl := range s.calls {
		in := map[string]any{}
		for k, v := range cl.input {
			if d, ok := v.(ago); ok {
				v = c.ago(time.Duration(d)).Format(time.RFC3339)
			}
			in[k] = v
		}
		rec := map[string]any{}
		if !cl.wildcard {
			for k, v := range cl.input {
				if !slices.Contains(cl.free, k) {
					rec[k] = v
				}
			}
		}
		var ob outObs
		if cl.tool == "reconcile" {
			// Explicit until before the wall clock: the tool's time.Now only
			// bounds the window, so the world can sit on the fixed clock and
			// the HH:MM bucket labels need no shift.
			fc := clock{now: fixedNow}
			ob = runTool(o, buildWorld(s.st, fc), cl.tool, in, fc)
		} else {
			ob = runTool(o, x, cl.tool, in, c)
		}
		if ob.Summary == "" && ob.Error == "" {
			return nil, nil, fmt.Errorf("%s %v: empty summary", cl.tool, in)
		}
		r := outRec{Tool: cl.tool, Summary: ob.Summary, Data: ob.Data, Truncated: ob.Truncated, Error: ob.Error}
		if len(rec) > 0 {
			r.Input = rec
		}
		if cl.tool == "reconcile" && s.narrow != nil {
			fc := clock{now: fixedNow}
			in2 := map[string]any{"table": "public.orders", "since": s.narrow[0].Format(time.RFC3339), "until": s.narrow[1].Format(time.RFC3339)}
			ob2 := runTool(o, buildWorld(s.st, fc), cl.tool, in2, fc)
			r = outRec{Tool: r.Tool, Input: r.Input, Sequence: []outObs{ob, ob2}}
		}
		out = append(out, r)
	}
	out = append(out, outRec{Tool: "similar_incidents", Summary: "No similar closed incident found on pipeline orders (3 past rooms searched)."})
	return out, x, nil
}

func (s scen) render(o *owners.Owners) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 5*time.Millisecond)))
		c := clock{now: time.Now().UTC().Truncate(time.Second)}
		out, err := s.renderAt(c, o)
		if err != nil {
			return nil, err
		}
		if time.Now().UTC().Truncate(time.Second).Equal(c.now) {
			return out, nil
		}
	}
	return nil, errors.New("could not generate within one second")
}

func (s scen) renderAt(c clock, o *owners.Owners) ([]byte, error) {
	b, err := os.ReadFile(s.file)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	root := doc.Content[0]
	recs, _, err := s.recordings(c, o)
	if err != nil {
		return nil, err
	}
	pm := recs[0]
	alert := child(root, "alert")
	if err := setKey(alert, "pipeline_map", pm.Summary); err != nil {
		return nil, err
	}
	child(alert, "pipeline_map").Style = yaml.LiteralStyle
	if err := setKey(root, "recordings", recs); err != nil {
		return nil, err
	}
	styleSummaries(child(root, "recordings"))
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	_ = enc.Close()
	return spaceTopLevel(out.Bytes()), nil
}

func child(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

func setKey(m *yaml.Node, key string, v any) error {
	var n yaml.Node
	if err := n.Encode(v); err != nil {
		return err
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			n.HeadComment, n.LineComment = m.Content[i+1].HeadComment, m.Content[i+1].LineComment
			m.Content[i+1] = &n
			return nil
		}
	}
	return fmt.Errorf("key %s missing", key)
}

func styleSummaries(n *yaml.Node) {
	if n == nil {
		return
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if v := n.Content[i+1]; n.Content[i].Value == "summary" && v.Kind == yaml.ScalarNode {
				v.Style = yaml.LiteralStyle
			}
		}
	}
	for _, c := range n.Content {
		if c.Kind == yaml.MappingNode || c.Kind == yaml.SequenceNode {
			styleSummaries(c)
		}
	}
}

func spaceTopLevel(b []byte) []byte {
	lines := strings.Split(string(b), "\n")
	var out []string
	for i, l := range lines {
		if i > 0 && l != "" && l[0] != ' ' && l[0] != '#' && l[0] != '-' && !strings.HasPrefix(l, "description:") {
			j := len(out)
			for j > 0 && strings.HasPrefix(out[j-1], "#") {
				j--
			}
			if j > 0 && out[j-1] != "" {
				out = append(out[:j], append([]string{""}, out[j:]...)...)
			}
		}
		out = append(out, l)
	}
	return []byte(strings.Join(out, "\n"))
}
