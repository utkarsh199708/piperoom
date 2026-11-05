package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Alerts for the scenarios the watcher cannot see (dev/scenarios.md). Each is
// what a team's own check would send: a row-count reconciliation between the
// source and Kafka or the warehouse. The body is stored verbatim in
// room.created.alert, so it says what was observed, never the cause.
const (
	missedTableAlert = `{
  "title": "orders: refunds not arriving in Kafka",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-missed-table",
  "source": "reconciliation",
  "description": "public.devctl_refunds has rows in the source database, but topic pg.public.devctl_refunds has no records. The connector reports RUNNING."
}`
	badDeployAlert = `{
  "title": "orders: new orders missing from analytics.orders",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-consumer-bad-deploy",
  "source": "reconciliation",
  "description": "Orders created in the last hour exist in the source and in topic pg.public.orders but not in analytics.orders. order-sync was redeployed shortly before and has since been stopped."
}`
	rowsLostAlert = `{
  "title": "orders: rows of public.orders missing from analytics.orders",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-rows-lost-after-retention",
  "source": "reconciliation",
  "description": "Orders written while order-sync was stopped for a node drain are in public.orders but not in analytics.orders. order-sync is running again with lag 0."
}`
	skippedRangeAlert = `{
  "title": "orders: rows of public.orders missing from analytics.orders",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-consumer-skipped-range",
  "source": "reconciliation",
  "description": "Orders written during the order-sync deploy are in public.orders but not in analytics.orders. order-sync is running again with lag 0."
}`
)

// alertOpts are the parsed `devctl alert` arguments.
type alertOpts struct {
	scenario string
	server   string
	asUser   string
}

func alertUsage() string {
	return "usage: devctl alert <scenario> [--server URL] [--as user] (scenarios with an alert: " + strings.Join(alertScenarios(), ", ") + ")"
}

// alertScenarios lists the scenarios that carry an alert, sorted.
func alertScenarios() []string {
	var out []string
	for _, n := range scenarioNames() {
		if scenarios[n].alert != "" {
			out = append(out, n)
		}
	}
	return out
}

// parseAlertFlags parses `devctl alert`; flags may come before or after the
// scenario.
func parseAlertFlags(args []string) (alertOpts, error) {
	var o alertOpts
	fs := flag.NewFlagSet("alert", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.server, "server", "", "server base URL (default http://PIPEROOM_HTTP_ADDR)")
	fs.StringVar(&o.asUser, "as", "arjun", "user id sent as X-User (dev auth mode)")
	var pos []string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return alertOpts{}, fmt.Errorf("%s: %w", alertUsage(), err)
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		rest = fs.Args()[1:]
	}
	if len(pos) != 1 {
		return alertOpts{}, errors.New(alertUsage())
	}
	o.scenario = pos[0]
	sc, ok := scenarios[o.scenario]
	if !ok {
		return alertOpts{}, fmt.Errorf("unknown scenario %q (%s)", o.scenario, strings.Join(scenarioNames(), ", "))
	}
	if sc.alert == "" {
		return alertOpts{}, fmt.Errorf("scenario %q has no alert: the watcher opens its room (signals %s)", o.scenario, strings.Join(sc.signals, ", "))
	}
	if strings.TrimSpace(o.asUser) == "" {
		return alertOpts{}, errors.New("devctl alert: --as must not be empty")
	}
	return o, nil
}

// alertCmd posts the scenario's alert to the server's dev webhook
// (POST /api/alerts/webhook, docs/api.md), which opens or joins the room. The
// webhook is unsigned and mounted in dev auth mode only. It takes no stack
// lock: it changes nothing on the stack.
func alertCmd(ctx context.Context, st *stack, args []string, out io.Writer) error {
	o, err := parseAlertFlags(args)
	if err != nil {
		return err
	}
	base := o.server
	if base == "" {
		base = serverURL(st.cfg.HTTPAddr)
	}
	return postAlert(ctx, &http.Client{Timeout: 10 * time.Second}, base, o.asUser, scenarios[o.scenario].alert, out)
}

func postAlert(ctx context.Context, hc *http.Client, base, user, body string, out io.Writer) error {
	rooms, err := newHTTPRooms(base, user) // validates the URL the same way
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rooms.base+"/api/alerts/webhook", bytes.NewReader([]byte(body)))
	if err != nil {
		return fmt.Errorf("devctl: alert: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User", user)
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("devctl: alert: POST /api/alerts/webhook: %w", err)
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, maxRehearseReply))
	if err != nil {
		return fmt.Errorf("devctl: alert: read reply: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK:
	case http.StatusNotFound:
		return errors.New("devctl: alert: POST /api/alerts/webhook: HTTP 404 (the unsigned webhook is mounted in dev auth mode only)")
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("devctl: alert: POST /api/alerts/webhook: HTTP %d (--as sends X-User, which needs dev auth mode and a user from dev/users.yaml)", resp.StatusCode)
	default:
		return fmt.Errorf("devctl: alert: POST /api/alerts/webhook: HTTP %d", resp.StatusCode)
	}
	var room struct {
		ID     string `json:"id"`
		RoomID string `json:"room_id"`
	}
	_ = json.Unmarshal(reply, &room) // the id is a courtesy; the status already says it worked
	id := room.ID
	if id == "" {
		id = room.RoomID
	}
	verb := "opened"
	if resp.StatusCode == http.StatusOK {
		verb = "joined"
	}
	if id == "" {
		id = "(id not in reply)"
	}
	fmt.Fprintf(out, "alert sent: %s room %s\n", verb, id)
	return nil
}
