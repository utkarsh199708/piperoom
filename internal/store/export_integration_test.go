//go:build integration

package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/gowebpki/jcs"

	"piperoom/internal/agent"
	"piperoom/internal/room"
)

// TestExportFromPostgresIsStableAndVerifies runs the audit export over the real
// store: jsonb reorders payload keys and truncates times to microseconds, and
// the hash must still be the same on every read and verify from the document.
func TestExportFromPostgresIsStableAndVerifies(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	svc := room.New(room.Deps{
		Store: s, Users: room.DefaultUsers(),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "Orders <stale> & late", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	if _, err := svc.AddComment(ctx, arjun, snap.ID, "", "z-key and a-key"); err != nil {
		t.Fatalf("comment: %v", err)
	}

	export := func() (string, []byte) {
		ex, err := svc.PrepareExport(ctx, snap.ID)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		var buf bytes.Buffer
		if err := ex.Write(ctx, &buf); err != nil {
			t.Fatalf("write: %v", err)
		}
		return ex.SHA256(), buf.Bytes()
	}
	sum1, body1 := export()
	sum2, body2 := export()
	if sum1 != sum2 || !bytes.Equal(body1, body2) {
		t.Fatalf("export of an unchanged room must be byte-identical")
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(body1, &members); err != nil {
		t.Fatal(err)
	}
	delete(members, "integrity")
	rest, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	canon, err := jcs.Transform(rest)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(canon)
	if hex.EncodeToString(h[:]) != sum1 {
		t.Fatalf("document does not verify against its hash")
	}

	if _, err := svc.AddComment(ctx, arjun, snap.ID, "", "later"); err != nil {
		t.Fatal(err)
	}
	if sum3, _ := export(); sum3 == sum1 {
		t.Fatalf("hash must change when an event is added")
	}
}
