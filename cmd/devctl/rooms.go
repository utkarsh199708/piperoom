package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const roomsUsage = "usage: devctl rooms delete <room-id> --reason <text> [--archive] [--as <admin>] [--server <url>]"

// roomsDeleteTimeout covers the server's own budget for DELETE (5 minutes,
// with the archive write) plus slack.
const roomsDeleteTimeout = 6 * time.Minute

// maxRoomsReply caps the reply body read (AGENTS.md rule 7).
const maxRoomsReply = 4 << 20

// roomsCmd runs `devctl rooms delete <id> --reason <text>`
// (docs/design/retention.md section 6). It calls DELETE /api/rooms/{id} on
// the running server, so the server's cache and the room's WebSockets go with
// the rows. A delete is an erase; --archive keeps a copy in the server's
// PIPEROOM_ARCHIVE_DIR first. --as sends X-User (dev mode); in oidc mode put
// the admin's session cookie ("name=value") in PIPEROOM_COOKIE. Only a
// users.yaml admin succeeds.
func roomsCmd(ctx context.Context, st *stack, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "delete" {
		return errors.New(roomsUsage)
	}
	fs := flag.NewFlagSet("rooms delete", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	reason := fs.String("reason", "", "why the room is deleted (stored in the audit row)")
	as := fs.String("as", "", "admin user id, sent as X-User (dev mode)")
	server := fs.String("server", "", "server base URL (default http://PIPEROOM_HTTP_ADDR)")
	archive := fs.Bool("archive", false, "write the room's export to the server's PIPEROOM_ARCHIVE_DIR before deleting")
	// The room id may come before or after the flags.
	var pos []string
	rest := args[1:]
	for {
		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("%s: %w", roomsUsage, err)
		}
		if fs.NArg() == 0 {
			break
		}
		pos = append(pos, fs.Arg(0))
		rest = fs.Args()[1:]
	}
	if len(pos) != 1 || pos[0] == "" || strings.TrimSpace(*reason) == "" {
		return errors.New(roomsUsage)
	}
	cookie := os.Getenv("PIPEROOM_COOKIE")
	if *as == "" && cookie == "" {
		return fmt.Errorf("devctl: say who deletes: --as <admin> (dev mode) or PIPEROOM_COOKIE (oidc mode)\n%s", roomsUsage)
	}
	base := *server
	if base == "" {
		base = serverURL(st.cfg.HTTPAddr)
	}
	cctx, cancel := context.WithTimeout(ctx, roomsDeleteTimeout)
	defer cancel()
	return deleteRoom(cctx, &http.Client{Timeout: roomsDeleteTimeout}, base, pos[0], *reason, *archive, *as, cookie, out)
}

// serverURL turns PIPEROOM_HTTP_ADDR into a base URL; an empty host means the
// loopback address.
func serverURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "http://" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// roomDeletion is the answer of DELETE /api/rooms/{id} (docs/api.md).
type roomDeletion struct {
	RoomID     string    `json:"room_id"`
	PipelineID string    `json:"pipeline_id"`
	DeletedBy  string    `json:"deleted_by"`
	DeletedAt  time.Time `json:"deleted_at"`
	EventCount int64     `json:"event_count"`
	Archived   bool      `json:"archived"`
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// deleteRoom sends the request and prints what the server did. The cookie is
// never printed or logged.
func deleteRoom(ctx context.Context, hc *http.Client, base, roomID, reason string, archive bool, as, cookie string, out io.Writer) error {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("devctl: bad server URL %q", base)
	}
	u = u.JoinPath("api", "rooms", roomID)
	q := url.Values{"reason": {reason}}
	if archive {
		q.Set("archive", "true")
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("devctl: build request: %w", err)
	}
	// Every non-GET under /api must say JSON (the server's CSRF guard).
	req.Header.Set("Content-Type", "application/json")
	if as != "" {
		req.Header.Set("X-User", as)
	}
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("devctl: delete room %s: %w", roomID, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRoomsReply))
	if err != nil {
		return fmt.Errorf("devctl: delete room %s: read reply: %w", roomID, err)
	}
	if resp.StatusCode != http.StatusOK {
		var ae apiError
		if json.Unmarshal(body, &ae) == nil && ae.Error.Code != "" {
			return fmt.Errorf("devctl: delete room %s: %d %s: %s", roomID, resp.StatusCode, ae.Error.Code, ae.Error.Message)
		}
		return fmt.Errorf("devctl: delete room %s: HTTP %d", roomID, resp.StatusCode)
	}
	var d roomDeletion
	if err := json.Unmarshal(body, &d); err != nil {
		return fmt.Errorf("devctl: delete room %s: decode reply: %w", roomID, err)
	}
	archived := "no archive"
	if d.Archived {
		archived = "archived first"
	}
	_, _ = fmt.Fprintf(out, "deleted room %s (pipeline %s): %d events; by %s at %s; %s\n",
		d.RoomID, d.PipelineID, d.EventCount, d.DeletedBy, d.DeletedAt.UTC().Format(time.RFC3339), archived)
	return nil
}
