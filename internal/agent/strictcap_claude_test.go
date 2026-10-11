//go:build claude

package agent

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/kafkatools"
)

// TestStrictCapLiveBisect sends the real tool list to the API with the first N
// tools marked strict, for falling N, and reports which N the API accepts. It
// exists because the compiled-grammar limit is not a published number: run it
// whenever a schema grows or a tool is added, and set maxStrictTools to the
// largest passing N. Needs ANTHROPIC_API_KEY; no dev stack. Each request asks
// for one output token, so a full run costs a few cents.
//
//	go test -tags claude -run TestStrictCapLiveBisect -v ./internal/agent
func TestStrictCapLiveBisect(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set")
	}
	reg := tools.NewRegistry()
	if err := dbtools.Register(reg, dbtools.Deps{Owners: &owners.Owners{}}); err != nil {
		t.Fatal(err)
	}
	if err := kafkatools.Register(reg, kafkatools.Deps{}); err != nil {
		t.Fatal(err)
	}
	areg := actions.NewRegistry()
	if err := actions.Register(areg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	room := newRoomTools(Deps{Actions: areg})
	reads := reg.All()
	client := anthropic.NewClient()
	model := config.Load().Model

	// Same slot rule as buildToolList: room tools first, then reads in order.
	build := func(n int) []anthropic.BetaToolUnionParam {
		var ps []anthropic.BetaToolUnionParam
		readStrict := max(n-len(room), 0)
		for i, rt := range reads {
			ps = append(ps, toolParam(rt, true, i < readStrict))
		}
		for i, rt := range room {
			ps = append(ps, toolParam(rt, false, i < n))
		}
		return ps
	}
	total := len(reads) + len(room)
	for _, n := range []int{min(total, 20), 16, 12, 10, 8, 6, 4, 3} {
		if n > total {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		_, err := client.Beta.Messages.New(ctx, anthropic.BetaMessageNewParams{
			Model:      model,
			MaxTokens:  1,
			Tools:      build(n),
			Messages:   []anthropic.BetaMessageParam{anthropic.NewBetaUserMessage(anthropic.NewBetaTextBlock("hi"))},
			ToolChoice: anthropic.BetaToolChoiceUnionParam{OfAuto: &anthropic.BetaToolChoiceAutoParam{}},
		})
		cancel()
		switch {
		case err == nil:
			t.Logf("strict=%2d of %d tools: OK", n, total)
		default:
			var apiErr *anthropic.Error
			msg := err.Error()
			if errors.As(err, &apiErr) {
				msg = apiErr.Error()
			}
			if i := strings.Index(msg, "\n"); i > 0 {
				msg = msg[:i]
			}
			t.Logf("strict=%2d of %d tools: REJECTED: %s", n, total, msg)
		}
	}
	t.Logf("current maxStrictTools=%d", maxStrictTools)
}
