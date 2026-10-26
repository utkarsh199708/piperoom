package evals

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

// Past rooms for the scenarios whose key hint is a closed room. The harness
// replays recordings and never runs similar_incidents, so a scenario carries
// the tool's answer as a recording. These fixtures are the history that answer
// was made from: TestSimilarIncidentsRecordings runs the real tool over them
// and fails when a recording drifts, which is how the recordings are
// regenerated (print res.Summary, paste it into the scenario).

func pastAt(m time.Month, day, hour int) time.Time {
	return time.Date(2026, m, day, hour, 0, 0, 0, time.UTC)
}

const pastSchemaTrace = "org.apache.kafka.common.errors.SerializationException: Error retrieving Avro schema for id 2\n" +
	"Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema 2 not found; error code: 40403"

const pastConnectSchemaTrace = `pg-orders-connector (source): RUNNING on connect:8083.
Task 0: FAILED:
org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value"; error code: 409`

// labelled is a person's room.labelled verdict. similar_incidents offers a
// closed room as a precedent only when it carries one; the closing note alone is
// free text nobody verified, so an unlabelled room is counted and left out.
func labelled(layer, cause, fix string) *events.RoomLabelledPayload {
	return &events.RoomLabelledPayload{ActualLayer: layer, Cause: cause, FixThatWorked: fix}
}

// historyFor returns the closed rooms the named scenario's recording was made from.
func historyFor(name string) []fake.Room {
	base := []fake.Room{
		{ID: "r_01J8N4Q2M7ZK3V9T6B5N8C1D0E", Title: "orders stale: connector task failed", ClosedAt: pastAt(8, 3, 11),
			Resolution:  "Debezium registered an incompatible Avro value schema after an ALTER COLUMN. Set compatibility of pg.public.orders-value to NONE for one registration, restarted task 0, then set BACKWARD again.",
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: pastConnectSchemaTrace}},
			Label: labelled("schema-registry", "Debezium registered an incompatible Avro value schema for pg.public.orders-value after an ALTER COLUMN and connector task 0 failed",
				"Set compatibility of pg.public.orders-value to NONE for one registration, restarted task 0, then set BACKWARD again")},
		{ID: "r_01J9A7C5D8XH2R4W6Y1E3F5G7H", Title: "order-sync lag growing on partition 1, pod restarting", ClosedAt: pastAt(8, 30, 9),
			Resolution: "A hand-written record without the Avro magic byte crash-looped order-sync. Skipped the offset after the consumer owner approved and added a dead-letter topic.",
			Label: labelled("consumer", "A hand-written record without the Avro magic byte crash-looped the order-sync consumer",
				"Skipped the poisoned offset after the consumer owner approved and added a dead-letter topic")},
	}
	switch name {
	case "recurring-schema-deleted":
		return append(base,
			fake.Room{ID: "r_01JA3M6S9T4V8W2X5Y7Z0B1C3D", Title: "order-sync failing: Schema 2 not found (error code 40403)", ClosedAt: pastAt(9, 15, 14),
				Resolution:  "A cleanup script hard-deleted schema version 1 (id 2) of pg.public.orders-value. Re-registered the same schema under its ORIGINAL id 2 by putting the subject in IMPORT mode, then back to READWRITE, and restarted order-sync. A normal register would have created a new id and left old records unreadable. Removed delete rights from the cleanup service account.",
				Handoff:     &events.HandoffSummary{WhatBroke: "order-sync could not decode records written with schema id 2.", Why: "Schema id 2 was hard-deleted from the registry.", Open: []string{"limit who can hard-delete schemas"}},
				ToolResults: []fake.ToolResult{{Tool: "kafka_consumer_lag", IsError: true, Summary: pastSchemaTrace}},
				Label: labelled("schema-registry", "A cleanup script hard-deleted schema version 1 (id 2) of pg.public.orders-value, so order-sync got Schema 2 not found (error code 40403)",
					"Re-registered the same schema under its original id 2 with the subject in IMPORT mode, back to READWRITE, restarted order-sync")},
		)
	case "recurring-connector-paused":
		return append(base,
			fake.Room{ID: "r_01JA9B2E4F6G8H1J3K5M7N9P0Q", Title: "analytics.orders stale for 3h, no failed tasks", ClosedAt: pastAt(9, 22, 6),
				Resolution: "The nightly maintenance job pauses pg-orders-connector at 02:00 and resumes it at 02:30. Its resume call timed out and the connector stayed PAUSED. Resumed the connector (safe: the slot stayed active, no data lost) and made the job retry the resume and alert if the connector is still paused.",
				Handoff:    &events.HandoffSummary{WhatBroke: "Nothing flowed after 02:00.", Why: "The maintenance job did not resume the connector.", Risks: []string{"WAL grows while paused"}},
				Label: labelled("kafka-connect", "pg-orders-connector stayed PAUSED: the nightly maintenance job's resume call timed out, so analytics.orders went stale with no failed task",
					"Resumed pg-orders-connector (the slot stayed active, no data lost) and made the job retry the resume")},
		)
	}
	return base
}

// recordedQueries is the query each recording was made from: what a careful
// agent searches once it has read the consumer owner's log line (or the
// connector state). The recording has no input, so it answers any query. The
// alert title alone would rank a poison-pill room first for the schema
// scenario, which is realistic and why the agent has to refine its query.
var recordedQueries = map[string]string{
	"recurring-schema-deleted":   "order-sync Error retrieving Avro schema for id 2: Schema 2 not found; error code: 40403",
	"recurring-connector-paused": "analytics.orders stale, pg-orders-connector PAUSED, no failed tasks",
}

// updateSimilar rewrites the similar_incidents summary of a scenario from the
// real tool: go test ./evals -run TestSimilarIncidentsRecordings
// -update-similar-recordings, then review the diff.
var updateSimilar = flag.Bool("update-similar-recordings", false, "rewrite the similar_incidents summary of the recurring-* scenarios from the real tool")

// rewriteSimilarSummary replaces the body of the first similar_incidents
// recording's "summary: |-" block in the scenario file.
func rewriteSimilarSummary(t *testing.T, path, summary string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	nl := "\n"
	if strings.Contains(string(raw), "\r\n") {
		nl = "\r\n"
	}
	lines := strings.Split(string(raw), nl)
	for i := range lines {
		if lines[i] != "  - tool: "+similarincidents.ToolName || i+1 >= len(lines) || lines[i+1] != "    summary: |-" {
			continue
		}
		end := i + 2
		for end < len(lines) && strings.HasPrefix(lines[end], "      ") {
			end++
		}
		var body []string
		for _, l := range strings.Split(summary, "\n") {
			body = append(body, "      "+l)
		}
		out := append(append(append([]string{}, lines[:i+2]...), body...), lines[end:]...)
		if err := os.WriteFile(path, []byte(strings.Join(out, nl)), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("%s: no similar_incidents recording with a |- summary", path)
}

func TestSimilarIncidentsRecordings(t *testing.T) {
	for name, query := range recordedQueries {
		t.Run(name, func(t *testing.T) {
			sc, err := LoadScenario(filepath.Join("scenarios", name+".yaml"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			st, err := fake.NewStore(ctx, historyFor(name)...)
			if err != nil {
				t.Fatal(err)
			}
			tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
			in, _ := json.Marshal(map[string]any{"query": query})
			res, err := tl.Run(tools.WithPipeline(ctx, owners.Pipeline{ID: sc.Pipeline.ID}), in)
			if err != nil {
				t.Fatal(err)
			}
			var rec *Recording
			for i := range sc.Recordings {
				if sc.Recordings[i].Tool == similarincidents.ToolName {
					rec = &sc.Recordings[i]
				}
			}
			if rec == nil {
				t.Fatalf("scenario %s has no similar_incidents recording", name)
			}
			if os.Getenv("PRINT_SIMILAR") != "" {
				t.Logf("summary:\n%s", res.Summary)
			}
			if rec.Summary != res.Summary && *updateSimilar {
				rewriteSimilarSummary(t, filepath.Join("scenarios", name+".yaml"), res.Summary)
				t.Logf("%s: similar_incidents summary rewritten", name)
			} else if rec.Summary != res.Summary {
				t.Errorf("recording drifted from the real tool.\nrecorded:\n%s\nreal:\n%s", rec.Summary, res.Summary)
			}
			// The past room that is the key hint must be the first hit.
			d := res.Data.(similarincidents.Data)
			if len(d.Hits) == 0 || d.Hits[0].RoomID != historyFor(name)[2].ID {
				t.Errorf("first hit = %+v, want the recurring incident", d.Hits)
			}
			if !contains(sc.Expect.EvidenceTools, similarincidents.ToolName) {
				t.Errorf("scenario does not require similar_incidents as evidence")
			}
		})
	}
}

// TestSimilarIncidentsDropUnlabelledRoom is the contamination case behind the
// label gate: the closed room that matches the query best has a confident
// closing note and no human label, and a second, weaker room is labelled. Only
// the labelled one may reach the agent; the unlabelled one is counted, not
// shown, and neither its text nor its fix appears in the summary or the data.
func TestSimilarIncidentsDropUnlabelledRoom(t *testing.T) {
	ctx := context.Background()
	const wrongFix = "Hard-delete the schema subject and truncate the topic"
	st, err := fake.NewStore(ctx,
		fake.Room{ID: "r_01JB0UNLABELLED00000000000A", Title: "order-sync failing: Schema 2 not found (error code 40403)", ClosedAt: pastAt(9, 15, 14),
			Resolution:  "Schema 2 not found. " + wrongFix + ".",
			ToolResults: []fake.ToolResult{{Tool: "kafka_consumer_lag", IsError: true, Summary: pastSchemaTrace}}},
		fake.Room{ID: "r_01JB0LABELLED0000000000000B", Title: "orders stale: connector task failed", ClosedAt: pastAt(8, 3, 11),
			Resolution: "Task 0 failed on an incompatible Avro schema; fixed by a registration with compatibility NONE.",
			Label: labelled("schema-registry", "Debezium registered an incompatible Avro value schema for pg.public.orders-value",
				"Set compatibility to NONE for one registration, restarted task 0, then back to BACKWARD")},
	)
	if err != nil {
		t.Fatal(err)
	}
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
	in, _ := json.Marshal(map[string]any{"query": recordedQueries["recurring-schema-deleted"] + " Avro schema incompatible"})
	res, err := tl.Run(tools.WithPipeline(ctx, owners.Pipeline{ID: "orders"}), in)
	if err != nil {
		t.Fatal(err)
	}
	d := res.Data.(similarincidents.Data)
	for _, h := range d.Hits {
		if h.RoomID == "r_01JB0UNLABELLED00000000000A" {
			t.Errorf("the unlabelled room is offered as a precedent: %+v", h)
		}
	}
	if d.UnlabelledSkipped != 1 {
		t.Errorf("unlabelled_skipped = %d, want 1", d.UnlabelledSkipped)
	}
	if strings.Contains(res.Summary, wrongFix) || strings.Contains(res.Summary, "r_01JB0UNLABELLED00000000000A") {
		t.Errorf("the summary carries the unlabelled room:\n%s", res.Summary)
	}
	b, _ := json.Marshal(d)
	if strings.Contains(string(b), wrongFix) {
		t.Errorf("the data carries the unlabelled room's closing note: %s", b)
	}
	if !strings.Contains(res.Summary, "no human label") {
		t.Errorf("the summary does not say matching rooms were left out for lacking a label:\n%s", res.Summary)
	}
}
