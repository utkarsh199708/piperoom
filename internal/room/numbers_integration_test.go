//go:build integration

package room

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/config"
	"piperoom/internal/jsonnum"
)

// jsonb keeps the decimal scale of a number, so the text it returns can be much
// longer than the text that went in: 1.<700 zeros>e-320 is 707 characters in and
// 1022 after the round trip. The classification must not depend on that length.
// Real Postgres: run the numbers through jsonb and classify both texts.
func TestNumberClassificationSurvivesJSONBRoundTrip(t *testing.T) {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.RoomDBURL)
	if err != nil {
		t.Skipf("room Postgres unreachable at %s: %v", cfg.RoomDBURL, err)
	}
	defer conn.Close(ctx)

	zeros := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = '0'
		}
		return string(b)
	}
	for name, tok := range map[string]string{
		"707 characters in, 1022 after":   "1." + zeros(700) + "e-320",
		"long exact 2^-20":                "9.5367431640625" + zeros(1200) + "e-7",
		"exponent form of an integer":     "1.152921504606846976e18",
		"long fraction on an exact value": "-0.5" + zeros(1500),
		"inexact above 2^53":              "9007199254740993." + zeros(1200),
		"short":                           "0.1",
	} {
		var back string
		if err := conn.QueryRow(ctx, `SELECT ($1::text)::jsonb::text`, `{"n":`+tok+`}`).Scan(&back); err != nil {
			t.Fatalf("%s: jsonb round trip: %v", name, err)
		}
		var after string
		jsonnum.Scan([]byte(back), func(s, e int) { after = back[s:e] })
		if after == "" {
			t.Fatalf("%s: no number in %q", name, back)
		}
		if name == "707 characters in, 1022 after" && (len(tok) != 707 || len(after) != 1022) {
			t.Fatalf("%s: lengths %d -> %d, want 707 -> 1022 (Postgres changed the text it returns?)", name, len(tok), len(after))
		}
		if before, got := classifyNumber(tok), classifyNumber(after); before != got {
			t.Errorf("%s: classified %d before the round trip and %d after (%d -> %d characters)", name, before, got, len(tok), len(after))
		}
	}

	// The 707-character example also executes as a whole body: the guard and the
	// hash form see the same thing before and after the round trip.
	body := `{"n":1.` + zeros(700) + `e-320}`
	var back string
	if err := conn.QueryRow(ctx, `SELECT ($1::text)::jsonb::text`, body).Scan(&back); err != nil {
		t.Fatalf("jsonb round trip of the body: %v", err)
	}
	_, inBefore := hashForm([]byte(body))
	_, inAfter := hashForm([]byte(back))
	if inBefore != inAfter {
		t.Errorf("hashForm counts %d inexact numbers before the round trip and %d after", inBefore, inAfter)
	}
	if hasOutOfRangeNumber([]byte(body)) != hasOutOfRangeNumber([]byte(back)) {
		t.Error("the out-of-range check differs before and after the round trip")
	}
}
