// Package forward is the one call path from the server's setup tools and
// setup action adapters (internal/setup/tools, internal/setup/actions) to the
// runner paired to a room: it picks the room, gives the request its id and
// deadline, executes it through the room's protocol.RunnerExecutor and turns a
// failed or oversized result into an error. It builds no params of its own:
// callers pass a request whose op and params they already checked against the
// catalogue (internal/setup/catalog).
//
// Nothing here logs: a result is data for the caller, and the runner already
// redacted it.
package forward

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// ErrNoRunner is protocol.ErrNoRunner: no executor, or no room to route to.
var ErrNoRunner = protocol.ErrNoRunner

// ErrOpFailed reports an op the runner ran (or refused inside its Validate)
// with OK false. The error text is the runner's redacted, capped summary.
var ErrOpFailed = errors.New("setup: the runner reported the op failed")

// ErrBadResult reports a result over the protocol's Data cap.
var ErrBadResult = errors.New("setup: unusable op result")

// Executor is protocol.RunnerExecutor; tests pass a fake.
type Executor interface {
	Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error)
}

// maxErrorText caps the runner's summary inside an error: the runner caps it
// at tools.MaxSummaryBytes already; this keeps an error line short.
const maxErrorText = 512

// Sender sends ops to the runner of one room.
type Sender struct {
	ex     Executor
	roomID string
}

// New returns a Sender over ex. roomID binds it to one room; "" takes the
// room from each call's context (room.RoomID), which is how an action call
// arrives. A nil ex makes every call answer ErrNoRunner.
func New(ex Executor, roomID string) *Sender {
	return &Sender{ex: ex, roomID: roomID}
}

// Room returns the room a call reaches: the bound one, else the call's. A
// call carrying another room than the bound one is refused: it would send this
// room's op to that room's runner.
func (s *Sender) Room(ctx context.Context) (string, error) {
	if s == nil || s.ex == nil {
		return "", ErrNoRunner
	}
	roomID := s.roomID
	ctxRoom := room.RoomID(ctx)
	switch {
	case roomID == "":
		roomID = ctxRoom
	case ctxRoom != "" && ctxRoom != roomID:
		return "", fmt.Errorf("the call belongs to another room than this setup stack's: %w", ErrNoRunner)
	}
	if roomID == "" {
		return "", ErrNoRunner
	}
	return roomID, nil
}

// Send executes req on roomID's runner. It sets req.ID and req.Deadline: the
// deadline is ctx's when it has one, else now plus def. It returns the result
// with an error when the executor failed (ErrNoRunner, a
// *protocol.RefusedError, ctx's error), when the result says OK false
// (ErrOpFailed, plus the sentinel its Data's error_code names), or when Data
// exceeds protocol.MaxDataBytes (ErrBadResult). The result is returned in
// every case so a caller can keep its receipt.
func (s *Sender) Send(ctx context.Context, roomID string, req protocol.OpRequest, def time.Duration) (protocol.OpResult, error) {
	if s == nil || s.ex == nil {
		return protocol.OpResult{}, ErrNoRunner
	}
	if _, ok := ctx.Deadline(); !ok && def > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, def)
		defer cancel()
	}
	if dl, ok := ctx.Deadline(); ok {
		req.Deadline = dl
	}
	id, err := newOpID()
	if err != nil {
		return protocol.OpResult{}, err
	}
	req.ID = id
	res, err := s.ex.Execute(ctx, roomID, req)
	if err != nil {
		if errors.Is(err, protocol.ErrNoRunner) {
			return res, ErrNoRunner
		}
		return res, err
	}
	if !res.OK {
		return res, failed(res)
	}
	if len(res.Data) > protocol.MaxDataBytes {
		return res, fmt.Errorf("%w: data is %d bytes, over the %d cap", ErrBadResult, len(res.Data), protocol.MaxDataBytes)
	}
	return res, nil
}

// failed turns an OK-false result into an error: the summary plus the
// sentinel its Data's error_code names, so a caller branches with errors.Is.
func failed(res protocol.OpResult) error {
	text := clip(strings.TrimSpace(res.Summary))
	if text == "" {
		text = "no summary"
	}
	var env struct {
		Code string `json:"error_code"`
	}
	if len(res.Data) > 0 && json.Unmarshal(res.Data, &env) == nil {
		switch env.Code {
		case "not_supported":
			return fmt.Errorf("%w: %s: %w", ErrOpFailed, text, clients.ErrNotSupported)
		case "not_found":
			return fmt.Errorf("%w: %s: %w", ErrOpFailed, text, clients.ErrNotFound)
		case "out_of_scope":
			return fmt.Errorf("%w: %s: %w", ErrOpFailed, text, tools.ErrOutOfScope)
		}
	}
	return fmt.Errorf("%w: %s", ErrOpFailed, text)
}

// newOpID returns a request id; the runner keys results and its ledger by it.
func newOpID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("setup: generating an op id: %w", err)
	}
	return "su_" + hex.EncodeToString(b[:]), nil
}

// clip cuts untrusted text on a rune boundary and drops control characters.
func clip(s string) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if (r < 0x20 && r != '\n') || r == 0x7f {
			return '?'
		}
		return r
	}, s)
	if len(s) <= maxErrorText {
		return s
	}
	cut := maxErrorText
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
