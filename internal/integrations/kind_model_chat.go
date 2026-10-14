package integrations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"piperoom/internal/integrations/spec"
	"piperoom/internal/slackbridge"
)

// Model and chat kind ids.
const (
	KindAnthropic = "anthropic"
	KindSlack     = "slack"
)

// DefaultModel is the model the Anthropic card tests when none is typed
// (config.Load's PIPEROOM_MODEL default).
const DefaultModel = "claude-opus-5-5"

func (d Deps) anthropicTest(ctx context.Context, v spec.Values) []spec.Check {
	ctx, cancel := testCtx(ctx, d.TestTimeout)
	defer cancel()
	cl := newChecklist(ctx, d.CheckTimeout, v)
	key := v.Secret("api_key")
	if key == "" {
		cl.add("input", "Form values are valid", fail("field api_key: required", ""))
		return cl.result()
	}
	model := strings.TrimSpace(v.Get("model"))
	if model == "" {
		model = DefaultModel
	}
	base := d.anthropicBase
	if base == "" {
		base = "https://api.anthropic.com"
	}
	hc, _, err := newHTTPClient(httpOpts{BaseURL: base, Dial: d.dial()})
	if err != nil {
		cl.add("input", "Form values are valid", fail(errText(err), ""))
		return cl.result()
	}
	hc.Timeout = 0
	defer hc.CloseIdleConnections()
	opts := []option.RequestOption{
		option.WithoutEnvironmentDefaults(), option.WithAPIKey(key.Reveal()),
		option.WithHTTPClient(hc), option.WithMaxRetries(0),
	}
	if d.anthropicBase != "" {
		opts = append(opts, option.WithBaseURL(d.anthropicBase))
	}
	client := anthropic.NewClient(opts...)
	cl.run("model", "A one-token request to "+model+" works", func(ctx context.Context) outcome {
		msg, err := client.Messages.New(ctx, anthropic.MessageNewParams{
			Model: model, MaxTokens: 1,
			Messages: []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("ping"))},
		})
		var apiErr *anthropic.Error
		switch {
		case errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusUnauthorized:
			return fail("the API key was rejected (HTTP 401)", "Create a key in the Claude Console (Settings → API keys) and paste it again.")
		case errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusForbidden:
			return fail("the key may not use this model or workspace (HTTP 403)", "Check the key's workspace and model access in the Claude Console.")
		case errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound:
			return fail("model "+model+" was not found (HTTP 404)", "Use a model id from the Claude docs' model list.")
		case errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusTooManyRequests:
			return warn("the key works but is rate limited right now (HTTP 429)", "")
		case errors.As(err, &apiErr):
			return fail(fmt.Sprintf("the API answered HTTP %d", apiErr.StatusCode), "")
		case err != nil:
			return httpFailure(err, "api.anthropic.com")
		}
		return pass("answered by " + msg.Model)
	})
	cl.add("live_swap", "The saved key is used without a restart", warn(
		"saved, but the running agent keeps using ANTHROPIC_API_KEY until the model endpoint accepts a key (agent.EndpointConfig has no API key field yet)",
		"Until then, also set ANTHROPIC_API_KEY (it may be a secret:// reference) and restart."))
	return cl.result()
}

func (d Deps) anthropicKind() spec.Kind {
	return spec.Kind{
		ID: KindAnthropic, Category: spec.CategoryModel, Title: "Anthropic",
		Summary: "Claude through the Anthropic API", Logo: "anthropic",
		Reads: []string{"nothing: Piperoom sends the investigation's tool results to the model"},
		Never: []string{"sends secrets or credentials to the model"},
		Fields: []spec.Field{
			{Name: "api_key", Label: "API key", Type: spec.FieldSecret, Required: true},
			{Name: "model", Label: "Model", Help: "Empty = " + DefaultModel, Type: spec.FieldText, Pattern: `^[A-Za-z0-9._:@/-]*$`},
		},
		Required: true, Test: d.anthropicTest,
		Build: func(context.Context, spec.Values) (any, func(), error) {
			return nil, nil, fmt.Errorf("%w: the model endpoint reads ANTHROPIC_API_KEY only (agent.EndpointConfig needs an APIKey field)", ErrUnsupported)
		},
	}
}

// slackCall POSTs one Web API method and decodes its JSON answer. Slack
// answers HTTP 200 with "ok": false for most errors.
func slackCall(ctx context.Context, c *http.Client, base *url.URL, token, method string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String()+"/"+method, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("integrations: slack request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("integrations: slack %s answered HTTP %d", method, resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out); err != nil {
		return fmt.Errorf("integrations: slack %s: bad JSON", method)
	}
	return nil
}

func (d Deps) slackTest(ctx context.Context, v spec.Values) []spec.Check {
	ctx, cancel := testCtx(ctx, d.TestTimeout)
	defer cancel()
	cl := newChecklist(ctx, d.CheckTimeout, v)
	token, channel := v.Secret("bot_token"), strings.TrimSpace(v.Get("channel_id"))
	if token == "" || channel == "" {
		cl.add("input", "Form values are valid", fail("bot token and channel id are required", ""))
		return cl.result()
	}
	c, base, err := newHTTPClient(httpOpts{BaseURL: d.slackBase, Dial: d.dial()})
	if err != nil {
		cl.add("input", "Form values are valid", fail(errText(err), ""))
		return cl.result()
	}
	c.Timeout = 0
	defer c.CloseIdleConnections()
	var botUser string
	st := cl.run("auth", "Bot token works (auth.test)", func(ctx context.Context) outcome {
		var r struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
			User  string `json:"user"`
			Team  string `json:"team"`
		}
		if err := slackCall(ctx, c, base, token.Reveal(), "auth.test", url.Values{}, &r); err != nil {
			return httpFailure(err, "slack.com")
		}
		if !r.OK {
			return fail("auth.test: "+r.Error, "Paste the app's Bot User OAuth Token (xoxb-…) from OAuth & Permissions.")
		}
		botUser = r.User
		return pass("bot " + r.User + " in " + r.Team)
	})
	if st != spec.CheckPass {
		cl.skip("channel", "Bot is in the channel", "skipped: the token does not work")
		return cl.result()
	}
	cl.run("channel", "Bot is a member of "+channel, func(ctx context.Context) outcome {
		var r struct {
			OK      bool   `json:"ok"`
			Error   string `json:"error"`
			Channel struct {
				Name      string `json:"name"`
				IsMember  bool   `json:"is_member"`
				IsPrivate bool   `json:"is_private"`
			} `json:"channel"`
		}
		if err := slackCall(ctx, c, base, token.Reveal(), "conversations.info", url.Values{"channel": {channel}}, &r); err != nil {
			return httpFailure(err, "slack.com")
		}
		switch {
		case !r.OK && r.Error == "missing_scope":
			return fail("the token lacks the scope to read channel info", "Add the channels:read and groups:read bot scopes, then reinstall the app.")
		case !r.OK && r.Error == "channel_not_found":
			return fail("channel "+channel+" was not found or the bot cannot see it", "Invite the bot to the channel: /invite @"+botUser)
		case !r.OK:
			return fail("conversations.info: "+r.Error, "")
		case !r.Channel.IsMember:
			return fail("the bot is not a member of #"+r.Channel.Name, "In #"+r.Channel.Name+", type /invite @"+botUser)
		case !r.Channel.IsPrivate:
			return warn("#"+r.Channel.Name+" is public; room threads repeat proposals and hand-offs", "Use a private channel for incident rooms.")
		}
		return pass("#" + r.Channel.Name)
	})
	return cl.result()
}

func (d Deps) slackKind() spec.Kind {
	return spec.Kind{
		ID: KindSlack, Category: spec.CategoryChat, Title: "Slack",
		Summary: "Mirror each room into one Slack thread (outbound only)", Logo: "slack",
		Reads: []string{"the channel's name and whether the bot is a member"},
		Never: []string{"reads messages", "acts on anything typed in Slack"},
		Fields: []spec.Field{
			{Name: "bot_token", Label: "Bot token", Help: "xoxb-…, with the chat:write scope", Type: spec.FieldSecret, Required: true},
			{Name: "channel_id", Label: "Channel ID", Help: "Like C0123ABC (channel details → bottom), not #name", Type: spec.FieldText, Required: true, Pattern: `^[CGD][A-Z0-9]{6,}$`},
		},
		Test: d.slackTest,
		// Build returns the bridge settings; the room service owns the
		// events store the bridge needs, so it calls slackbridge.New.
		Build: func(_ context.Context, v spec.Values) (any, func(), error) {
			c, _, err := newHTTPClient(httpOpts{BaseURL: d.slackBase, Dial: d.dial()})
			if err != nil {
				return nil, nil, err
			}
			o := slackbridge.Options{Token: v.Secret("bot_token"), ChannelID: strings.TrimSpace(v.Get("channel_id")), HTTPClient: c}
			if d.slackBase != "https://slack.com/api" {
				o.APIBaseURL = d.slackBase
			}
			return o, c.CloseIdleConnections, nil
		},
	}
}
