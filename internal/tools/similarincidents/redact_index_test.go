package similarincidents_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/tools/similarincidents/fake"
)

// TestSecretsRedactedAtIndexTime: a room closed before the room log was
// redacted (#96) still holds raw secrets in its hand-off, resolution and tool
// results. A query that guesses such a secret must not match the room (a hit
// would confirm the guess), while the room's other words still match.
func TestSecretsRedactedAtIndexTime(t *testing.T) {
	const secret = "Zq9xT4mLw2Kp8"
	trace := "org.apache.kafka.connect.errors.ConnectException: cannot connect to postgres://app:" + secret + "@db:5432/orders\n" +
		"Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed password=" + secret
	st := seeded(t, fake.Room{ID: "r-1", Title: "sink auth zzauthword", ClosedAt: day(9, 1),
		Resolution: "rotated password=" + secret,
		Handoff: &events.HandoffSummary{
			WhatBroke: "sink login failed",
			Why:       "the DSN postgres://app:" + secret + "@db:5432/orders was stale",
			Open:      []string{"check password=" + secret + " in vault"},
		},
		ToolResults: []fake.ToolResult{{Tool: "connect_status", Summary: trace}},
	})
	tl := similarincidents.New(similarincidents.Deps{Source: similarincidents.FromStore(st)})
	// The bare secret matches nothing.
	res, err := run(t, tl, scoped("orders"), input(secret, 3))
	if err != nil {
		t.Fatal(err)
	}
	if ids := hitIDs(t, res); len(ids) != 0 {
		t.Errorf("query %q matched %v", secret, ids)
	}
	// In context the other words match, but the right guess scores exactly
	// like a wrong one of the same length, so nothing is confirmed.
	const wrong = "Wr0ngGuess77X"
	for _, form := range []string{"password=%s", "postgres://app:%s@db:5432/orders", "ConnectException: cannot connect to postgres://app:%s@db:5432/orders"} {
		right, err := run(t, tl, scoped("orders"), input(fmt.Sprintf(form, secret), 3))
		if err != nil {
			t.Fatal(err)
		}
		guess, err := run(t, tl, scoped("orders"), input(fmt.Sprintf(form, wrong), 3))
		if err != nil {
			t.Fatal(err)
		}
		r, g := right.Data.(similarincidents.Data), guess.Data.(similarincidents.Data)
		if !reflect.DeepEqual(r.Hits, g.Hits) {
			t.Errorf("%q: right guess %+v, wrong guess %+v", form, r.Hits, g.Hits)
		}
		for _, h := range r.Hits {
			if strings.Contains(h.Resolution, secret) || strings.Contains(h.Title, secret) {
				t.Errorf("secret shown in hit %+v", h)
			}
		}
	}
	res, err = run(t, tl, scoped("orders"), input("zzauthword", 3))
	if err != nil {
		t.Fatal(err)
	}
	if ids := hitIDs(t, res); len(ids) != 1 {
		t.Errorf("control query: hits = %v, want r-1", ids)
	}
}
