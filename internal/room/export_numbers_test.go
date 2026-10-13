package room_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/gowebpki/jcs"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// fixedStore returns a memstore whose clock never moves, with one room opened
// at a fixed time. Everything in it is deterministic.
func fixedStore(t *testing.T) (*memstore.Store, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 13, 9, 30, 0, 0, time.UTC)
	st := memstore.NewWithClock(func() time.Time { return at })
	if err := st.CreateRoom(context.Background(), store.Room{
		ID: "r_fixed", Title: "Orders stale", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	return st, at
}

func exportOf(t *testing.T, st store.Store, roomID string) (string, []byte) {
	t.Helper()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	return exportRoomFor(t, svc, roomID)
}

// A room that already holds a number outside double range (stored before the
// ingest guard existed; jsonb accepts 1e400) must still export, and the export
// must verify. The number is hashed as its exact text and counted.
func TestExportOutOfRangeNumberVerifiesAndIsCounted(t *testing.T) {
	st, _ := fixedStore(t)
	ctx := context.Background()
	human := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	if _, err := st.Append(ctx, "r_fixed", events.RoomCreated, human, events.RoomCreatedPayload{
		Title: "Orders stale", Severity: "sev2", PipelineID: "orders",
		Alert: json.RawMessage(`{"huge":1e400,"neg":-2.5E+999,"big":9007199254740993,"ok":0.1,"n":3}`),
	}); err != nil {
		t.Fatal(err)
	}
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	sum, body := exportRoomFor(t, svc, "r_fixed")

	// The document shows the numbers exactly as stored.
	for _, want := range []string{`"huge":1e400`, `"neg":-2.5E+999`, `"big":9007199254740993`, `"ok":0.1`} {
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("document must show %s as stored: %s", want, body)
		}
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(body, &members); err != nil {
		t.Fatal(err)
	}
	var integ map[string]any
	if err := json.Unmarshal(members["integrity"], &integ); err != nil {
		t.Fatal(err)
	}
	// 1e400, -2.5E+999 (out of range) and 9007199254740993 (above 2^53), once each.
	if integ["inexact_numbers"] != float64(3) || integ["sha256"] != sum {
		t.Fatalf("integrity = %v", integ)
	}

	// Independent re-hash: the out-of-range tokens become strings holding their
	// exact text, then plain jcs.Transform.
	delete(members, "integrity")
	rest, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	rest = bytes.Replace(rest, []byte(`:1e400,`), []byte(`:"1e400",`), 1)
	rest = bytes.Replace(rest, []byte(`:-2.5E+999,`), []byte(`:"-2.5E+999",`), 1)
	canon, err := jcs.Transform(rest)
	if err != nil {
		t.Fatalf("jcs: %v", err)
	}
	h := sha256.Sum256(canon)
	if hex.EncodeToString(h[:]) != sum {
		t.Fatalf("independent re-hash %s != %s", hex.EncodeToString(h[:]), sum)
	}
}

func TestExportInexactNumbersZeroForPlainDocument(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	sum, body := exportRoomFor(t, svc, roomID)
	doc := verifyExport(t, sum, body)
	integ := doc["integrity"].(map[string]any)
	if integ["inexact_numbers"].(json.Number).String() != "0" {
		t.Fatalf("a document with plain numbers has no inexact ones: %v", integ)
	}
	if integ["covers"] != "SHA-256 of the RFC 8785 (JCS) form of this document with the top-level \"integrity\" member removed. "+
		"It is not the hash of the file bytes; sha256sum of the file will differ." {
		t.Fatalf("covers wording: %v", integ["covers"])
	}
	if integ["numbers"] != "JCS writes numbers as IEEE 754 doubles; this document shows numbers exactly as stored. "+
		"Numbers a double cannot hold exactly are covered only at double precision, and numbers outside double range "+
		"are hashed as their exact text. See inexact_numbers." {
		t.Fatalf("numbers wording: %v", integ["numbers"])
	}
}

// fixedVectorDoc builds the one document the test vector below pins.
func fixedVectorDoc(t *testing.T) (string, []byte) {
	t.Helper()
	st, _ := fixedStore(t)
	ctx := context.Background()
	human := events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
	if _, err := st.Append(ctx, "r_fixed", events.RoomCreated, human, events.RoomCreatedPayload{
		Title: "Orders stale", Severity: "sev2", PipelineID: "orders",
		Alert: json.RawMessage(`{"z":1,"a":{"b":0.1,"c":[1,2.50,1e3]},"s":"<&> é","big":9007199254740993,"huge":1e400}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, "r_fixed", events.CommentAdded, human, events.CommentAddedPayload{
		CommentID: "c_1", Text: "slot is retaining WAL",
	}); err != nil {
		t.Fatal(err)
	}
	return exportOf(t, st, "r_fixed")
}

// Known document, known hash. The literal below was computed outside Go, by a
// small Node script (sorted keys, ECMAScript number text, 1e400 as a string),
// from the document this test builds, so a change to the canonicalization or to
// the document shape fails here instead of silently changing every hash.
// Recomputed the same way when events.Actor gained "verified" (A-ID-2); the
// same script still gives the old value 03cab4dd...31a5 for the old shape.
const fixedVectorSHA256 = "8b96e714f1b92f212e550fbde70484b9342f863fc67684bb4c8f2a79d4c65ff1"

func TestExportFixedVector(t *testing.T) {
	sum, body := fixedVectorDoc(t)
	if p := os.Getenv("PIPEROOM_DUMP_VECTOR"); p != "" {
		_ = os.WriteFile(p, body, 0o600)
	}
	if sum != fixedVectorSHA256 {
		t.Fatalf("hash of the fixed document = %s, want %s", sum, fixedVectorSHA256)
	}
	if !bytes.Contains(body, []byte(`"inexact_numbers":2`)) {
		t.Fatalf("the vector holds 9007199254740993 and 1e400: want inexact_numbers 2")
	}
}
