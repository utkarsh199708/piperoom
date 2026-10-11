package slackbridge

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/events"
)

const (
	maxBodyBytes  = 1 << 20
	maxSkew       = 5 * time.Minute
	commentBudget = 2 * time.Second // Slack wants an ack within 3 s
)

// CommentPoster writes a human comment into a room (the room service's
// comment path, so its own checks apply).
type CommentPoster interface {
	AddComment(ctx context.Context, roomID string, actor events.Actor, text string) error
}

// UserMapper maps a Slack user id to a Piperoom user. Unknown users get
// ok=false and their replies are ignored: no anonymous writes into a room.
type UserMapper interface {
	Resolve(slackUserID string) (events.Actor, bool)
}

// Handler receives Slack Events API callbacks.
type Handler struct {
	signingSecret []byte
	channel       string
	bridge        *Bridge
	comments      CommentPoster
	users         UserMapper
	now           func() time.Time
	log           *slog.Logger
}

// NewHandler builds the inbound handler. Mount it at e.g. POST /slack/events.
func NewHandler(signingSecret, channel string, b *Bridge, c CommentPoster, u UserMapper, log *slog.Logger) *Handler {
	return &Handler{signingSecret: []byte(signingSecret), channel: channel, bridge: b,
		comments: c, users: u, now: time.Now, log: log}
}

var errBadSignature = errors.New("slackbridge: bad signature")

func (h *Handler) verify(r *http.Request, body []byte) error {
	ts := r.Header.Get("X-Slack-Request-Timestamp")
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return errBadSignature
	}
	if d := h.now().Sub(time.Unix(sec, 0)); d > maxSkew || d < -maxSkew {
		return errBadSignature // replay protection
	}
	mac := hmac.New(sha256.New, h.signingSecret)
	mac.Write([]byte("v0:" + ts + ":"))
	mac.Write(body)
	want := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(r.Header.Get("X-Slack-Signature"))) {
		return errBadSignature
	}
	return nil
}

type envelope struct {
	Type      string `json:"type"`
	Challenge string `json:"challenge"`
	Event     struct {
		Type     string `json:"type"`
		Subtype  string `json:"subtype"`
		BotID    string `json:"bot_id"`
		User     string `json:"user"`
		Text     string `json:"text"`
		Channel  string `json:"channel"`
		ThreadTS string `json:"thread_ts"`
		TS       string `json:"ts"`
	} `json:"event"`
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil || len(body) > maxBodyBytes {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := h.verify(r, body); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	switch env.Type {
	case "url_verification":
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, env.Challenge)
	case "event_callback":
		// Slack re-sends on slow acks; the first delivery already ran.
		if r.Header.Get("X-Slack-Retry-Num") == "" {
			h.onMessage(r.Context(), env)
		}
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (h *Handler) onMessage(ctx context.Context, env envelope) {
	ev := env.Event
	// Only plain human replies inside a mirrored thread; never bots (our own
	// posts included), edits or deletions.
	if ev.Type != "message" || ev.Subtype != "" || ev.BotID != "" || ev.User == "" ||
		ev.Channel != h.channel || ev.ThreadTS == "" || ev.ThreadTS == ev.TS {
		return
	}
	text := strings.TrimSpace(ev.Text)
	if text == "" {
		return
	}
	roomID, ok := h.bridge.threads.RoomByThread(ev.ThreadTS)
	if !ok {
		return
	}
	actor, ok := h.users.Resolve(ev.User)
	if !ok {
		h.log.Info("slack reply from unmapped user ignored", "room", roomID)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, commentBudget)
	defer cancel()
	undo := h.bridge.markInbound(roomID, actor.ID, text)
	if err := h.comments.AddComment(ctx, roomID, actor, text); err != nil {
		undo()
		h.log.Warn("slack reply to comment failed", "room", roomID, "err", err)
	}
}
