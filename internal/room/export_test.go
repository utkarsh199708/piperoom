package room_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/gowebpki/jcs"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// exportRoomFor runs the whole export of a room and returns the header hash and
// the document bytes.
func exportRoomFor(t *testing.T, svc *room.Service, roomID string) (string, []byte) {
	t.Helper()
	ex, err := svc.PrepareExport(context.Background(), roomID)
	if err != nil {
		t.Fatalf("prepare export: %v", err)
	}
	var buf bytes.Buffer
	if err := ex.Write(context.Background(), &buf); err != nil {
		t.Fatalf("write export: %v", err)
	}
	return ex.SHA256(), buf.Bytes()
}

// verifyExport is what an auditor does: parse, drop "integrity", canonicalise,
// hash, compare with both the header value and integrity.sha256.
func verifyExport(t *testing.T, headerSum string, body []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("export is not valid JSON: %v", err)
	}
	integ, ok := doc["integrity"].(map[string]any)
	if !ok {
		t.Fatalf("export has no integrity member: %v", doc)
	}
	if integ["algorithm"] != "sha256" || integ["sha256"] != headerSum {
		t.Fatalf("integrity does not match the header hash: %v vs %s", integ, headerSum)
	}
	if got := jcsHashWithoutIntegrity(t, body); got != headerSum {
		t.Fatalf("recomputed hash %s != %s", got, headerSum)
	}
	return doc
}

// jcsHashWithoutIntegrity is an independent re-hash: it does not use the
// service's canonicalizer, only jcs.Transform on the document minus "integrity".
func jcsHashWithoutIntegrity(t *testing.T, body []byte) string {
	t.Helper()
	var members map[string]json.RawMessage
	if err := json.Unmarshal(body, &members); err != nil {
		t.Fatalf("export is not a JSON object: %v", err)
	}
	delete(members, "integrity")
	rest, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	canon, err := jcs.Transform(rest)
	if err != nil {
		t.Fatalf("jcs: %v", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:])
}

// TestExportAlertWithDuplicateKeys: the alert body is stored verbatim and may
// repeat a key; the export must still work and verify (JCS alone rejects it).
func TestExportAlertWithDuplicateKeys(t *testing.T) {
	svc, _, _ := newTestService(t)
	raw := json.RawMessage(`{"title":"x","dup":1,"dup":2,"big":9007199254740993}`)
	snap, _, err := svc.CreateRoomFromAlert(context.Background(), "x", "sev2", "orders", raw)
	if err != nil {
		t.Fatalf("alert: %v", err)
	}
	sum, body := exportRoomFor(t, svc, snap.ID)
	verifyExport(t, sum, body)
	if !bytes.Contains(body, []byte("9007199254740993")) {
		t.Fatalf("the document must show the number as stored")
	}
}

// TestExportCanonicalizationIsJCS pins the name stored in the document and the
// documented number limit: integers above 2^53 are hashed as IEEE doubles.
func TestExportCanonicalizationIsJCS(t *testing.T) {
	svc, _, _, roomID := richRoom(t)
	sum, body := exportRoomFor(t, svc, roomID)
	doc := verifyExport(t, sum, body)
	if got := doc["integrity"].(map[string]any)["canonicalization"]; got != "RFC 8785 (JCS)" {
		t.Fatalf("canonicalization = %v", got)
	}
}

// richRoom builds a room with a proposal (two steps, two layers, both approved,
// both executed), a comment, a steer suggestion, a hand-off that is accepted,
// and a close.
func richRoom(t *testing.T) (*room.Service, *memstore.Store, *clock, string) {
	t.Helper()
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	wh := &fakeAction{name: "wh_act", layer: "warehouse"}
	svc, st, c := newTestService(t, pg, wh)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID

	if _, err := svc.AddComment(ctx, mustUser(t, svc, "kabir"), roomID, "", "looks like <b>schema</b> & drift"); err != nil {
		t.Fatalf("comment: %v", err)
	}
	if _, err := svc.Steer(ctx, mustUser(t, svc, "kabir"), roomID, "check the connector"); err != nil {
		t.Fatalf("suggest: %v", err)
	}
	pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "widen column",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage(`{"table":"public.orders","n":12345678901234567890}`), Description: "alter"},
			{Action: "wh_act", Layer: "warehouse", Params: json.RawMessage(`{"table":"orders"}`), Description: "alter sink"},
		},
		RequiredLayers: []string{"postgres", "warehouse"},
		Risk:           "low", Rollback: "narrow it again",
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("proposal: %v", err)
	}
	c.Advance(time.Minute)
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve pg: %v", err)
	}
	c.Advance(time.Minute)
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "meera"), pid, "warehouse"); err != nil {
		t.Fatalf("approve wh: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(ctx, pid)
		return err == nil && p.Status == "executed"
	})

	hev, err := svc.CreateHandoff(ctx, mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	hp, _ := events.Decode[events.HandoffCreatedPayload](hev)
	if _, err := svc.AcceptHandoff(ctx, mustUser(t, svc, "priya"), hp.HandoffID); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "priya"), roomID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
	return svc, st, c, roomID
}

func TestExportShape(t *testing.T) {
	svc, st, _, roomID := richRoom(t)
	sum, body := exportRoomFor(t, svc, roomID)
	doc := verifyExport(t, sum, body)

	if doc["schema"] != room.ExportSchema {
		t.Fatalf("schema: %v", doc["schema"])
	}

	// Events: every one, in seq order, with actor and time, as stored.
	stored := allEvents(t, st, roomID)
	evs := doc["events"].([]any)
	if len(evs) != len(stored) || doc["event_count"].(json.Number).String() != strconv.Itoa(len(stored)) {
		t.Fatalf("want %d events, got %d (event_count %v)", len(stored), len(evs), doc["event_count"])
	}
	for i, raw := range evs {
		e := raw.(map[string]any)
		if e["seq"].(json.Number).String() != strconv.Itoa(i+1) {
			t.Fatalf("event %d out of order: seq %v", i, e["seq"])
		}
		actor := e["actor"].(map[string]any)
		if actor["id"] == "" || actor["kind"] == "" || e["at"] == "" || e["type"] == "" {
			t.Fatalf("event %d lacks actor/time/type: %v", i, e)
		}
	}

	// The comment's "<" ">" "&" must be stored as typed, not as <.
	if !bytes.Contains(body, []byte("<b>schema</b> & drift")) {
		t.Fatalf("html characters were escaped in the export")
	}
	// Numbers stay exactly as stored (no float rounding).
	if !bytes.Contains(body, []byte("12345678901234567890")) {
		t.Fatalf("large number was altered")
	}

	// Proposals carry the frozen params.
	props := doc["proposals"].([]any)
	if len(props) != 1 {
		t.Fatalf("want 1 proposal, got %d", len(props))
	}
	p := props[0].(map[string]any)
	steps := p["steps"].([]any)
	params := steps[0].(map[string]any)["params"].(map[string]any)
	if params["table"] != "public.orders" || p["status"] != "executed" || p["created_by"].(map[string]any)["id"] != "agent" {
		t.Fatalf("bad proposal: %v", p)
	}

	// Approvals: approver and time.
	appr := doc["approvals"].([]any)
	if len(appr) != 2 {
		t.Fatalf("want 2 approvals, got %d", len(appr))
	}
	a0 := appr[0].(map[string]any)
	if a0["layer"] != "postgres" || a0["decision"] != "approved" || a0["approver"].(map[string]any)["id"] != "priya" || a0["at"] == "" {
		t.Fatalf("bad approval: %v", a0)
	}
	if appr[1].(map[string]any)["approver"].(map[string]any)["id"] != "meera" {
		t.Fatalf("bad second approval: %v", appr[1])
	}

	// Action results.
	res := doc["action_results"].([]any)
	if len(res) != 2 || res[0].(map[string]any)["ok"] != true || res[1].(map[string]any)["step_index"].(json.Number).String() != "1" {
		t.Fatalf("bad action results: %v", res)
	}

	// Hand-off summary.
	hos := doc["handoffs"].([]any)
	if len(hos) != 1 {
		t.Fatalf("want 1 handoff, got %d", len(hos))
	}
	h := hos[0].(map[string]any)
	if h["status"] != "accepted" || h["to"].(map[string]any)["id"] != "priya" ||
		h["summary"].(map[string]any)["what_broke"] != "schema change" || h["accepted_by"].(map[string]any)["id"] != "priya" {
		t.Fatalf("bad handoff: %v", h)
	}
	if _, ok := h["summary"].(map[string]any)["done"].([]any); !ok {
		t.Fatalf("summary.done must be an array, got %v", h["summary"])
	}

	// Room block.
	r := doc["room"].(map[string]any)
	if r["id"] != roomID || r["status"] != "closed" || r["resolution"] != "fixed" || r["closed_at"] == nil {
		t.Fatalf("bad room block: %v", r)
	}
}

func TestExportHashStableAndChangesWithNewEvent(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	ctx := context.Background()

	sum1, body1 := exportRoomFor(t, svc, roomID)
	sum2, body2 := exportRoomFor(t, svc, roomID)
	if sum1 != sum2 || !bytes.Equal(body1, body2) {
		t.Fatalf("same room exported twice must give identical bytes and hash: %s vs %s", sum1, sum2)
	}
	verifyExport(t, sum1, body1)

	if _, err := svc.AddComment(ctx, mustUser(t, svc, "kabir"), roomID, "", "one more thing"); err != nil {
		t.Fatalf("comment: %v", err)
	}
	sum3, body3 := exportRoomFor(t, svc, roomID)
	if sum3 == sum1 {
		t.Fatalf("hash must change when an event is added")
	}
	verifyExport(t, sum3, body3)
}

func TestExportEmptySectionsAreArrays(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	sum, body := exportRoomFor(t, svc, roomID)
	doc := verifyExport(t, sum, body)
	for _, k := range []string{"proposals", "approvals", "action_results", "handoffs", "events"} {
		if _, ok := doc[k].([]any); !ok {
			t.Fatalf("%s must be a JSON array even when empty, got %#v", k, doc[k])
		}
	}
	if doc["room"].(map[string]any)["status"] != "open" {
		t.Fatalf("open room exported as %v", doc["room"])
	}
}

func TestExportUnknownRoomIs404(t *testing.T) {
	svc, _, _ := newTestService(t)
	_, err := svc.PrepareExport(context.Background(), "r_nope")
	wantCode(t, err, "not_found")
}

func TestExportTooLargeIs413(t *testing.T) {
	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, ExportMaxEvents: 1})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID // 2 events
	_, err := svc.PrepareExport(context.Background(), roomID)
	wantCode(t, err, "export_too_large")
	var re *room.Error
	if !errors.As(err, &re) || re.Status != 413 {
		t.Fatalf("want 413, got %v", err)
	}
}

// TestExportIsFixedAtPrepare: events appended after PrepareExport are not in
// the document, and the document still verifies against the prepared hash.
func TestExportIsFixedAtPrepare(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	ex, err := svc.PrepareExport(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddComment(context.Background(), mustUser(t, svc, "kabir"), roomID, "", "late"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := ex.Write(context.Background(), &buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	doc := verifyExport(t, ex.SHA256(), buf.Bytes())
	if n := len(doc["events"].([]any)); n != 2 {
		t.Fatalf("want the 2 events present at prepare time, got %d", n)
	}
}

// tamperStore changes what the second reading of the log returns.
type tamperStore struct {
	*memstore.Store
	hideLast bool // drop the newest event
	mutate   bool // change event 1's payload
}

func (s *tamperStore) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	evs, err := s.Store.Since(ctx, roomID, after, limit)
	if err != nil {
		return nil, err
	}
	if s.hideLast && len(evs) > 0 {
		evs = evs[:len(evs)-1]
	}
	if s.mutate {
		for i := range evs {
			if evs[i].Seq == 1 {
				evs[i].Payload = json.RawMessage(`{"title":"tampered"}`)
			}
		}
	}
	return evs, nil
}

func TestExportDetectsChangedSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(*tamperStore)
	}{
		{"event missing", func(s *tamperStore) { s.hideLast = true }},
		{"event altered", func(s *tamperStore) { s.mutate = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := &tamperStore{Store: memstore.New()}
			svc := room.New(room.Deps{Store: ts, Users: room.DefaultUsers(), Run: fakeRun})
			t.Cleanup(svc.Close)
			roomID := createRoom(t, svc, "arjun").ID
			ex, err := svc.PrepareExport(context.Background(), roomID)
			if err != nil {
				t.Fatal(err)
			}
			tc.do(ts)
			var buf bytes.Buffer
			err = ex.Write(context.Background(), &buf)
			if !errors.Is(err, room.ErrExportChanged) {
				t.Fatalf("want ErrExportChanged, got %v", err)
			}
		})
	}
}

func TestExportOfPurgedRoomIs404(t *testing.T) {
	svc, st, c := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), roomID, "done"); err != nil {
		t.Fatal(err)
	}
	c.Advance(48 * time.Hour)
	if _, err := svc.PurgeExpired(context.Background(), c.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("purge: %v", err)
	}
	_, err := svc.PrepareExport(context.Background(), roomID)
	wantCode(t, err, "not_found")
	_ = st
}

var _ store.Store = (*tamperStore)(nil)
