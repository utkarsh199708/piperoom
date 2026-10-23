package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/slackbridge"
	"piperoom/internal/store/memstore"
)

// X4d: the bridge off gives no pager (page-team answers slack_off); on, the
// page goes to the team's channel, not the room thread's, with the change's
// URL and service from its after JSON.
func TestTeamPagerWiring(t *testing.T) {
	var off *mirrors
	if off.teamPager() != nil {
		t.Fatal("a disabled bridge must give a nil room.TeamPager")
	}
	slack := newFakeSlack(t)
	m, err := newMirrors(context.Background(), memstore.New(), slackbridge.NewMemThreads(),
		slackSettings{Token: fakeToken, ChannelID: "CROOMS1", PublicURL: "https://pr.example", apiBase: slack.srv.URL}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.stop(mirrorStopTimeout) })
	after, _ := json.Marshal(clients.ExternalChange{URL: "https://ci.example/7", Service: "checkout-api"})
	page := room.TeamPage{
		RoomID: "r_1", PipelineID: "orders", Team: "checkout", Channel: "C0123ABCD",
		By: events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}, Scope: events.ScopeUpstreamApp,
		Hypothesis: "deploy <@U1> dropped a column", Confidence: "high", Steps: []int{2}, ChangeID: 9,
		Change: &clients.PipelineChange{ID: 9, Kind: clients.ChangeDeploy, Summary: "release 7", After: after, At: time.Now()},
	}
	if err := m.teamPager().PageTeam(context.Background(), page); err != nil {
		t.Fatal(err)
	}
	posts, _ := slack.snapshot()
	if len(posts) != 1 || posts[0]["channel"] != "C0123ABCD" {
		t.Fatalf("posts = %v, want one to the team's channel", posts)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(posts[0]["blocks"])
	raw := buf.Bytes()
	for _, want := range []string{"<https://ci.example/7|Open the change>", "checkout-api", "&lt;@U1&gt;", "<https://pr.example/rooms/r_1|Open room>"} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("page lacks %q: %s", want, raw)
		}
	}

	// A bridge error comes back as a room.TeamPageError with a code.
	page.Channel = "#checkout"
	err = m.teamPager().PageTeam(context.Background(), page)
	var pe *room.TeamPageError
	if !errors.As(err, &pe) || pe.Code != "bad_channel" {
		t.Fatalf("err = %v, want TeamPageError bad_channel", err)
	}
}
