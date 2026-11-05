package openlineage

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/topology"
	"piperoom/internal/version"
)

// Schema URLs of the vendored spec versions (docs/design/lineage.md section
// 6; testdata/ holds the files and the tests validate against them).
const (
	runEventSchemaURL      = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
	jobTypeSchemaURL       = "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
	dataSourceSchemaURL    = "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet"
	schemaSchemaURL        = "https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"
	columnLineageSchemaURL = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet"
	tagsSchemaURL          = "https://openlineage.io/spec/facets/1-0-0/TagsDatasetFacet.json#/$defs/TagsDatasetFacet"
	ownershipSchemaURL     = "https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json#/$defs/OwnershipDatasetFacet"
	// actionFacetSchemaURL names Piperoom's own run facet; the schema is
	// testdata/PiperoomActionRunFacet.json.
	actionFacetSchemaURL = "urn:piperoom:spec:facets:1-0-0:PiperoomActionRunFacet.json#/$defs/PiperoomActionRunFacet"

	tagSource = "PIPEROOM"
)

type runEvent struct {
	EventType string    `json:"eventType"`
	EventTime string    `json:"eventTime"`
	Producer  string    `json:"producer"`
	SchemaURL string    `json:"schemaURL"`
	Run       run       `json:"run"`
	Job       job       `json:"job"`
	Inputs    []dataset `json:"inputs"`
	Outputs   []dataset `json:"outputs"`
}

type run struct {
	RunID  string     `json:"runId"`
	Facets *runFacets `json:"facets,omitempty"`
}

type runFacets struct {
	Action *actionFacet `json:"piperoom_action,omitempty"`
}

type facet struct {
	Producer  string `json:"_producer"`
	SchemaURL string `json:"_schemaURL"`
}

type actionFacet struct {
	facet
	PipelineID string `json:"pipeline_id"`
	ProposalID string `json:"proposal_id"`
	StepIndex  int    `json:"step_index"`
}

type job struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Facets    struct {
		JobType jobTypeFacet `json:"jobType"`
	} `json:"facets"`
}

type jobTypeFacet struct {
	facet
	ProcessingType  string           `json:"processingType"`
	Integration     string           `json:"integration"`
	JobType         string           `json:"jobType"`
	EmissionPattern *emissionPattern `json:"emissionPattern,omitempty"`
}

type emissionPattern struct {
	EventTrigger     string `json:"eventTrigger"`
	EventContentMode string `json:"eventContentMode"`
	WindowDuration   int    `json:"windowDuration"`
}

type dataset struct {
	Namespace string        `json:"namespace"`
	Name      string        `json:"name"`
	Facets    datasetFacets `json:"facets"`
}

type datasetFacets struct {
	DataSource    *dataSourceFacet    `json:"dataSource,omitempty"`
	Schema        *schemaFacet        `json:"schema,omitempty"`
	ColumnLineage *columnLineageFacet `json:"columnLineage,omitempty"`
	Tags          *tagsFacet          `json:"tags,omitempty"`
	Ownership     *ownershipFacet     `json:"ownership,omitempty"`
}

type dataSourceFacet struct {
	facet
	Name string `json:"name"`
	URI  string `json:"uri,omitempty"`
}

type schemaFacet struct {
	facet
	Fields []schemaField `json:"fields"`
}

type schemaField struct {
	Name string `json:"name"`
}

type columnLineageFacet struct {
	facet
	Fields map[string]columnLineageField `json:"fields"`
}

type columnLineageField struct {
	InputFields []inputField `json:"inputFields"`
}

type inputField struct {
	Namespace       string           `json:"namespace"`
	Name            string           `json:"name"`
	Field           string           `json:"field"`
	Transformations []transformation `json:"transformations"`
}

type transformation struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype"`
	Description string `json:"description,omitempty"`
	Masking     bool   `json:"masking"`
}

type tagsFacet struct {
	facet
	Tags []tag `json:"tags"`
}

type tag struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Source string `json:"source,omitempty"`
	Field  string `json:"field,omitempty"`
}

type ownershipFacet struct {
	facet
	Owners []owner `json:"owners"`
}

type owner struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// producer is BaseEvent.producer and every facet's _producer, a URI:
// urn:piperoom:<version>, with anything a URN cannot hold replaced.
func producer() string {
	v := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune(".-_+", r) {
			return r
		}
		return '-'
	}, version.String())
	return "urn:piperoom:" + v
}

func newFacet(schemaURL string) facet { return facet{Producer: producer(), SchemaURL: schemaURL} }

// newRunID returns a UUID v7: 48 bits of Unix milliseconds, then random
// bits from crypto/rand, so run ids sort by time as the OpenLineage
// clients' do.
func newRunID(now time.Time) string {
	var b [16]byte
	_, _ = rand.Read(b[6:]) // crypto/rand.Read never fails (Go 1.24+)
	var ms [8]byte
	binary.BigEndian.PutUint64(ms[:], uint64(now.UnixMilli()))
	copy(b[:6], ms[2:])
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func newEvent(eventType string, now time.Time, namespace, jobName string, jt jobTypeFacet) runEvent {
	ev := runEvent{
		EventType: eventType,
		EventTime: now.UTC().Format(time.RFC3339Nano),
		Producer:  producer(),
		SchemaURL: runEventSchemaURL,
		Run:       run{RunID: newRunID(now)},
		Inputs:    []dataset{},
		Outputs:   []dataset{},
	}
	ev.Job.Namespace, ev.Job.Name = namespace, jobName
	jt.facet = newFacet(jobTypeSchemaURL)
	ev.Job.Facets.JobType = jt
	return ev
}

// topologyEvent maps one built graph to a RunEvent (section 6.1): source
// tables in, every other dataset out, with schema, column lineage, tags and
// ownership facets. Health, facts and every value stay out.
func topologyEvent(g topology.Graph, ns Namespaces, namespace string, now time.Time) runEvent {
	ev := newEvent("COMPLETE", now, namespace, "piperoom.pipeline."+g.PipelineID, jobTypeFacet{
		ProcessingType: "STREAMING", Integration: "PIPEROOM", JobType: "PIPELINE",
		EmissionPattern: &emissionPattern{EventTrigger: "PERIODIC", EventContentMode: "COMPLETE_SNAPSHOT",
			WindowDuration: int(config.OpenLineageEmitInterval / time.Second)},
	})
	type ref struct{ ns, name string }
	refs := map[string]ref{}
	kinds := map[string]topology.NodeKind{}
	for _, n := range g.Nodes {
		kinds[n.ID] = n.Kind
		if s, name, ok := ns.dataset(n); ok {
			refs[n.ID] = ref{s, name}
		}
	}
	// into: a node's incoming data edges; fields: each dataset's columns,
	// as the column maps name them, in first-seen order.
	into := map[string][]topology.Edge{}
	fields := map[string][]string{}
	addField := func(id, f string) {
		if f != "" && !slices.Contains(fields[id], f) {
			fields[id] = append(fields[id], f)
		}
	}
	for _, e := range g.Edges {
		if e.IsDependency() {
			continue
		}
		into[e.To] = append(into[e.To], e)
		for _, c := range e.Columns {
			if c.Transform != topology.ColumnDropped {
				addField(e.To, c.To)
			}
		}
	}
	for _, e := range g.Edges {
		for _, c := range e.Columns {
			addField(c.Input, c.From)
		}
	}
	for _, n := range g.Nodes {
		r, ok := refs[n.ID]
		if !ok {
			continue
		}
		d := dataset{Namespace: r.ns, Name: r.name}
		ds := newFacet(dataSourceSchemaURL)
		d.Facets.DataSource = &dataSourceFacet{facet: ds, Name: r.ns}
		if strings.Contains(r.ns, "://") {
			d.Facets.DataSource.URI = r.ns
		}
		if fs := fields[n.ID]; len(fs) > 0 {
			sf := &schemaFacet{facet: newFacet(schemaSchemaURL)}
			for _, f := range fs {
				sf.Fields = append(sf.Fields, schemaField{Name: f})
			}
			d.Facets.Schema = sf
		}
		cl := map[string]columnLineageField{}
		for _, e := range into[n.ID] {
			for _, c := range e.Columns {
				if c.To == "" || c.Transform == topology.ColumnDropped {
					continue // a dropped column has no output slot
				}
				if c.Transform == topology.ColumnAdded {
					if _, ok := cl[c.To]; !ok {
						cl[c.To] = columnLineageField{InputFields: []inputField{}}
					}
					continue
				}
				in, ok := refs[c.Input]
				if !ok || c.From == "" {
					continue // an input this process cannot name
				}
				f := cl[c.To]
				f.InputFields = append(f.InputFields, inputField{Namespace: in.ns, Name: in.name, Field: c.From,
					Transformations: []transformation{{Type: "DIRECT", Subtype: "IDENTITY", Description: describe(c)}}})
				cl[c.To] = f
			}
		}
		if len(cl) > 0 {
			d.Facets.ColumnLineage = &columnLineageFacet{facet: newFacet(columnLineageSchemaURL), Fields: cl}
		}
		if tags := nodeTags(n.Tags); len(tags) > 0 {
			d.Facets.Tags = &tagsFacet{facet: newFacet(tagsSchemaURL), Tags: tags}
		}
		if n.Owner != "" {
			d.Facets.Ownership = &ownershipFacet{facet: newFacet(ownershipSchemaURL),
				Owners: []owner{{Name: "user:" + n.Owner, Type: "MAINTAINER"}}}
		}
		if hasUpstreamDataset(n.ID, into, kinds) {
			ev.Outputs = append(ev.Outputs, d)
		} else {
			ev.Inputs = append(ev.Inputs, d)
		}
	}
	return ev
}

// hasUpstreamDataset reports whether a dataset node is fed, through data
// edges and processing nodes (connectors, groups), by another dataset.
func hasUpstreamDataset(id string, into map[string][]topology.Edge, kinds map[string]topology.NodeKind) bool {
	seen := map[string]bool{id: true}
	stack := []string{id}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, e := range into[cur] {
			if seen[e.From] {
				continue
			}
			if topology.IsDataset(kinds[e.From]) {
				return true
			}
			seen[e.From] = true
			stack = append(stack, e.From)
		}
	}
	return false
}

func describe(c topology.ColumnMap) string {
	var parts []string
	if c.Transform == topology.ColumnRename || c.Transform == topology.ColumnUnwrap {
		parts = append(parts, string(c.Transform))
	}
	if c.Mapping == topology.MappingInferred {
		parts = append(parts, "inferred (name match)")
	}
	return strings.Join(parts, ", ")
}

// nodeTags maps the documented governance tags; any other key stays home.
func nodeTags(t map[string]string) []tag {
	var out []tag
	if v := t[topology.TagPII]; v != "" {
		if pii, err := topology.ParsePIITag(v); err == nil {
			cols := make([]string, 0, len(pii))
			for c := range pii {
				cols = append(cols, c)
			}
			slices.Sort(cols)
			for _, c := range cols {
				out = append(out, tag{Key: topology.TagPII, Value: string(pii[c]), Field: c, Source: tagSource})
			}
		}
	}
	if v := t[topology.TagPIIOmitted]; v != "" {
		out = append(out, tag{Key: topology.TagPIIOmitted, Value: v})
	}
	if v := t[topology.TagRetention]; v != "" {
		out = append(out, tag{Key: topology.TagRetention, Value: v, Source: tagSource})
	}
	return out
}

// actionEvent maps one executed step (section 6.1): COMPLETE when ok, FAIL
// when not, OTHER when the outcome is unknown; outputs are the step's topic
// and table params that name a dataset of the pipeline's last graph. The
// step's output text is never read.
func actionEvent(pipelineID string, step events.ProposalStep, res events.ActionExecutedPayload, known map[string]dataset, namespace string, now time.Time) runEvent {
	typ := "FAIL"
	switch {
	case res.OutcomeUnknown:
		typ = "OTHER"
	case res.OK:
		typ = "COMPLETE"
	}
	ev := newEvent(typ, now, namespace, "piperoom."+step.Action, jobTypeFacet{
		ProcessingType: "BATCH", Integration: "PIPEROOM", JobType: "ACTION"})
	ev.Run.Facets = &runFacets{Action: &actionFacet{facet: newFacet(actionFacetSchemaURL),
		PipelineID: pipelineID, ProposalID: res.ProposalID, StepIndex: res.StepIndex}}
	topics, tables := stepTargets(step.Params)
	var keys []string
	for _, t := range topics {
		keys = append(keys, "topic:"+t)
	}
	for _, t := range tables {
		switch {
		case step.Layer == "postgres":
			keys = append(keys, "source:"+t)
		case step.Layer == "warehouse":
			keys = append(keys, "sink:"+t)
		case known["source:"+t].Name == "" || known["sink:"+t].Name == "":
			// Another layer's table param: named only when the map holds one
			// table by that name, never a guess between source and sink.
			keys = append(keys, "source:"+t, "sink:"+t)
		}
	}
	for _, k := range keys {
		if d, ok := known[k]; ok && !slices.ContainsFunc(ev.Outputs, func(o dataset) bool { return o.Namespace == d.Namespace && o.Name == d.Name }) {
			ev.Outputs = append(ev.Outputs, d)
		}
	}
	return ev
}

// stepTargets returns the topic and table names a step's params hold.
func stepTargets(params json.RawMessage) (topics, tables []string) {
	var p map[string]json.RawMessage
	if json.Unmarshal(params, &p) != nil {
		return nil, nil
	}
	read := func(keys ...string) []string {
		var out []string
		for _, k := range keys {
			var one string
			var many []string
			if json.Unmarshal(p[k], &one) == nil && one != "" {
				out = append(out, one)
			} else if json.Unmarshal(p[k], &many) == nil {
				out = append(out, many...)
			}
		}
		return out
	}
	return read("topic", "to_topic", "dlq_topic", "topics"), read("table", "tables")
}

// knownDatasets indexes a graph's nameable datasets as "topic:<name>",
// "source:<name>" and "sink:<name>", for the action events: only what the
// pipeline's map holds is ever named.
func knownDatasets(g topology.Graph, ns Namespaces) map[string]dataset {
	out := map[string]dataset{}
	for _, n := range g.Nodes {
		s, name, ok := ns.dataset(n)
		if !ok {
			continue
		}
		d := dataset{Namespace: s, Name: name}
		d.Facets.DataSource = &dataSourceFacet{facet: newFacet(dataSourceSchemaURL), Name: s}
		if strings.Contains(s, "://") {
			d.Facets.DataSource.URI = s
		}
		switch n.Kind {
		case topology.KindTopic, topology.KindDLQTopic:
			out["topic:"+n.Name] = d
		case topology.KindSourceTable:
			out["source:"+n.Name] = d
		case topology.KindSinkTable:
			out["sink:"+n.Name] = d
		}
	}
	return out
}
