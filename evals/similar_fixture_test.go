package evals

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

// historyFor returns the closed rooms the named scenario's recording was made from.
func historyFor(name string) []fake.Room {
	base := []fake.Room{
		{ID: "r_01J8N4Q2M7ZK3V9T6B5N8C1D0E", Title: "orders stale: connector task failed", ClosedAt: pastAt(8, 3, 11),
			Resolution: "Debezium registered an incompatible Avro value schema after an ALTER COLUMN. Set compatibility of pg.public.orders-value to NONE for one registration, restarted task 0, then set BACKWARD again.",
			Layer:      "kafka-connect", LabelTerms: []string{"schema compatibility"},
			ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: pastConnectSchemaTrace}}},
		{ID: "r_01J9A7C5D8XH2R4W6Y1E3F5G7H", Title: "order-sync lag growing on partition 1, pod restarting", ClosedAt: pastAt(8, 30, 9),
			Resolution: "A hand-written record without the Avro magic byte crash-looped order-sync. Skipped the offset after the consumer owner approved and added a dead-letter topic.",
			Layer:      "consumer", LabelTerms: []string{"poison pill"}},
	}
	switch name {
	case "recurring-schema-deleted":
		return append(base,
			fake.Room{ID: "r_01JA3M6S9T4V8W2X5Y7Z0B1C3D", Title: "order-sync failing: Schema 2 not found (error code 40403)", ClosedAt: pastAt(9, 15, 14),
				Resolution: "A cleanup script hard-deleted schema version 1 (id 2) of pg.public.orders-value. Re-registered the same schema under its ORIGINAL id 2 by putting the subject in IMPORT mode, then back to READWRITE, and restarted order-sync. A normal register would have created a new id and left old records unreadable. Removed delete rights from the cleanup service account.",
				Layer:      "schema-registry", LabelTerms: []string{"schema deleted", "original schema id"},
				Handoff:     &events.HandoffSummary{WhatBroke: "order-sync could not decode records written with schema id 2.", Why: "Schema id 2 was hard-deleted from the registry.", Open: []string{"limit who can hard-delete schemas"}},
				ToolResults: []fake.ToolResult{{Tool: "kafka_consumer_lag", IsError: true, Summary: pastSchemaTrace}}},
		)
	case "recurring-connector-paused":
		return append(base,
			fake.Room{ID: "r_01JA9B2E4F6G8H1J3K5M7N9P0Q", Title: "analytics.orders stale for 3h, no failed tasks", ClosedAt: pastAt(9, 22, 6),
				Resolution: "The nightly maintenance job pauses pg-orders-connector at 02:00 and resumes it at 02:30. Its resume call timed out and the connector stayed PAUSED. Resumed the connector (safe: the slot stayed active, no data lost) and made the job retry the resume and alert if the connector is still paused.",
				Layer:      "kafka-connect", LabelTerms: []string{"paused connector", "maintenance job"},
				Handoff: &events.HandoffSummary{WhatBroke: "Nothing flowed after 02:00.", Why: "The maintenance job did not resume the connector.", Risks: []string{"WAL grows while paused"}}},
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
			if rec.Summary != res.Summary {
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

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}
