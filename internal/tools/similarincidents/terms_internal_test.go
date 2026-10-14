package similarincidents

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools/similarincidents/fake"
)

// TestLabelTermsAreClippedInCountAndBytes: label terms are human text of up to
// 20 x 200 bytes; the cache keeps at most maxTerms of maxTermBytes each.
func TestLabelTermsAreClippedInCountAndBytes(t *testing.T) {
	ctx := context.Background()
	var terms []string
	for i := range 20 {
		terms = append(terms, fmt.Sprintf("term%02d%s", i, strings.Repeat("x", 190)))
	}
	st, err := fake.NewStore(ctx, fake.Room{ID: "r-1", Title: "t", ClosedAt: time.Now().Add(-time.Hour), Resolution: "r",
		Shadow: true, Layer: "kafka", LabelTerms: terms})
	if err != nil {
		t.Fatal(err)
	}
	rooms, err := st.ListClosedRooms(ctx, "orders", 5)
	if err != nil || len(rooms) != 1 {
		t.Fatalf("rooms = %v, %v", rooms, err)
	}
	pr, err := loadRoom(ctx, FromStore(st), rooms[0], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if pr.Layer != "kafka" {
		t.Fatalf("layer = %q, the label was not read", pr.Layer)
	}
	if len(pr.Terms) != maxTerms {
		t.Errorf("kept %d terms, want %d", len(pr.Terms), maxTerms)
	}
	for _, x := range pr.Terms {
		if len(x) > maxTermBytes {
			t.Errorf("term of %d bytes, cap %d", len(x), maxTermBytes)
		}
	}
	if len(pr.Terms) > 0 && !strings.HasPrefix(pr.Terms[0], "term00") {
		t.Errorf("first term = %q, order must be kept", pr.Terms[0])
	}
}
