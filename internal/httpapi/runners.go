package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
)

// WithSetup mounts the setup runner endpoint (GET /api/runners/ws) when
// enabled is true (PIPEROOM_SETUP_ENABLED=on). Without it the endpoint is not
// mounted and answers 404 not_found like any unknown route.
func WithSetup(enabled bool) Option {
	return func(s *Server) { s.setupEnabled = enabled }
}

// pairReadTimeout bounds the wait for the runner's first frame (the Pair).
const pairReadTimeout = 30 * time.Second

// handleRunnerWS serves GET /api/runners/ws: the one WebSocket to a paired
// setup runner (docs/design/setup-agent.md §2.1). The credential is the
// pairing code (or, later, the runner token) in the first frame, never a
// session: the runner dials outbound and has no cookie.
//
// The frame types and their checks are protocol's; this handler only performs
// the server-side order: Pair (validate, rate limit, audit) → Welcome → Hello
// (attach, token) → then the read loop (pong, refused, lost).
func (s *Server) handleRunnerWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.wsOriginPatterns()})
	if err != nil {
		return // Accept already wrote the response
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(protocol.MaxFrameBytes)

	connCtx, cancel := context.WithCancel(s.runnerCtx)
	defer cancel()

	source := s.clientIP(r)

	// The runner's first frame is a Pair. Nothing is told about the room until
	// the code checks out.
	pctx, pcancel := context.WithTimeout(connCtx, pairReadTimeout)
	pair, err := readRunnerFrame(pctx, conn)
	pcancel()
	if err != nil {
		closeRunner(conn, websocket.StatusPolicyViolation, "pair frame required")
		return
	}
	if pair.Type != protocol.FramePair || pair.Pair == nil {
		closeRunner(conn, websocket.StatusPolicyViolation, "pair frame required")
		return
	}
	if pair.Pair.Code == "" {
		// A token reconnect is U1-5a-2; before it exists the runner must pair
		// with a fresh code.
		closeRunner(conn, websocket.StatusPolicyViolation, "token reconnect not supported yet")
		return
	}
	if v, ok := pair.Pair.Version(); ok {
		if err := protocol.ServerAccepts(protocol.Current(), v); err != nil {
			_ = s.svc.EmitSetup(r.Context(), "", events.SetupPairing, events.SetupPairingPayload{
				Outcome: events.PairingRefused, Source: source,
			})
			closeRunner(conn, protocol.CloseProtocolVersion, err.Error())
			return
		}
	}

	res, err := s.svc.PairRunner(r.Context(), pair.Pair.Code, source)
	if err != nil {
		s.log.Error("httpapi: runner pairing failed", "source", source, "err", err)
		closeRunner(conn, websocket.StatusInternalError, "pairing failed")
		return
	}
	if res.Outcome != events.PairingOK {
		closeRunner(conn, websocket.StatusPolicyViolation, "pairing refused: "+res.Outcome)
		return
	}

	snap, err := s.svc.GetRoom(r.Context(), res.RoomID)
	if err != nil {
		s.log.Error("httpapi: runner pairing room lookup failed", "room", res.RoomID, "err", err)
		closeRunner(conn, websocket.StatusInternalError, "room lookup failed")
		return
	}

	welcome := protocol.Welcome{
		RoomID:          res.RoomID,
		Title:           snap.Title,
		PipelineID:      snap.PipelineID,
		Tenant:          res.Tenant,
		Creator:         res.Creator,
		ProtocolVersion: protocol.CurrentMajor,
		ProtocolMinor:   protocol.CurrentMinor,
	}
	if err := writeRunnerFrame(connCtx, conn, protocol.Frame{Type: protocol.FrameWelcome, Welcome: &welcome}); err != nil {
		return
	}

	// Hello: the runner's description, sent after the person confirmed on its
	// terminal. The server always re-checks the version on it. A runner that
	// never sends Hello is as silent as one that never pings, so the wait is
	// bounded by protocol.LostAfter.
	hctx, hcancel := context.WithTimeout(connCtx, protocol.LostAfter)
	hello, err := readRunnerFrame(hctx, conn)
	hcancel()
	if err != nil {
		return
	}
	if hello.Type != protocol.FrameHello || hello.Hello == nil {
		closeRunner(conn, websocket.StatusPolicyViolation, "hello frame required")
		return
	}
	if err := protocol.ServerAccepts(protocol.Current(), hello.Hello.Version()); err != nil {
		_ = s.svc.EmitSetup(r.Context(), "", events.SetupPairing, events.SetupPairingPayload{
			Outcome: events.PairingRefused, Source: source,
		})
		closeRunner(conn, protocol.CloseProtocolVersion, err.Error())
		return
	}

	token, tokenExpires, err := newRunnerToken()
	if err != nil {
		s.log.Error("httpapi: runner token generation failed", "err", err)
		closeRunner(conn, websocket.StatusInternalError, "token generation failed")
		return
	}
	rn := room.Runner{
		Tenant:           res.Tenant,
		RoomID:           res.RoomID,
		Creator:          res.Creator,
		Label:            hello.Hello.Label,
		OS:               hello.Hello.OS,
		Arch:             hello.Hello.Arch,
		Tools:            hello.Hello.Tools,
		ProjectDir:       hello.Hello.ProjectDir,
		CatalogueVersion: hello.Hello.CatalogueVersion,
		ProtocolVersion:  hello.Hello.ProtocolVersion,
		ProtocolMinor:    hello.Hello.ProtocolMinor,
		Token:            token,
		TokenExpiresAt:   tokenExpires,
	}
	if err := s.svc.AttachRunner(r.Context(), pair.Pair.Code, rn); err != nil {
		s.log.Error("httpapi: runner attach failed", "room", res.RoomID, "err", err)
		closeRunner(conn, websocket.StatusInternalError, "attach failed")
		return
	}
	if err := writeRunnerFrame(connCtx, conn, protocol.Frame{Type: protocol.FrameToken, Token: &protocol.Token{
		Token: token, ExpiresAt: tokenExpires.UTC(),
	}}); err != nil {
		return
	}

	// The runner is attached; serve until it goes away. A runner silent for
	// protocol.LostAfter is setup.runner_lost. pong replies to ping; refused
	// records setup.runner_refused; other frame types are dropped by the
	// caller's own check.
	s.runnerLoop(connCtx, conn, res.Tenant, res.RoomID, res.Creator.ID, token)
}

// runnerLoop reads frames for an attached runner until it is silent for
// protocol.LostAfter or the connection ends. token is the attachment identity
// handed to the registry so a superseded socket never detaches or touches the
// replacement.
func (s *Server) runnerLoop(ctx context.Context, conn *websocket.Conn, tenant, roomID, creatorID, token string) {
	for {
		rctx, rcancel := context.WithTimeout(ctx, protocol.LostAfter)
		f, err := readRunnerFrame(rctx, conn)
		rcancel()
		if err != nil {
			// An unknown frame type (a newer minor) is dropped without acting
			// on its payload and the socket stays open.
			if errors.Is(err, protocol.ErrUnknownFrame) {
				continue
			}
			if errors.Is(err, protocol.ErrMalformedFrame) {
				closeRunner(conn, websocket.StatusPolicyViolation, "malformed frame")
			} else if errors.Is(err, context.DeadlineExceeded) {
				closeRunner(conn, websocket.StatusPolicyViolation, "runner lost")
			}
			s.svc.RunnerLost(ctx, tenant, roomID, creatorID, token)
			return
		}
		s.svc.RunnerTouch(tenant, roomID, creatorID, token)
		switch f.Type {
		case protocol.FramePing:
			_ = writeRunnerFrame(ctx, conn, protocol.Frame{Type: protocol.FramePong})
		case protocol.FrameRefused:
			s.svc.RunnerRefused(ctx, tenant, roomID, creatorID, token, events.SetupRunnerRefusedPayload{
				Op:         f.Refused.Op,
				Reason:     f.Refused.Reason,
				ProposalID: f.Refused.ProposalID,
				StepIndex:  f.Refused.StepIndex,
			})
		}
		// FrameResult, FrameResume and the rest are consumed by U1-5a-2
		// (RunnerExecutor / Resume reconciliation); unknown types are already
		// dropped by Frame.Check in readRunnerFrame.
	}
}

func readRunnerFrame(ctx context.Context, conn *websocket.Conn) (protocol.Frame, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return protocol.Frame{}, err
	}
	if typ != websocket.MessageText {
		return protocol.Frame{}, fmt.Errorf("httpapi: runner sent a non-text frame")
	}
	var f protocol.Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return protocol.Frame{}, fmt.Errorf("httpapi: runner frame is not JSON: %w", err)
	}
	if err := f.Check(); err != nil {
		return protocol.Frame{}, err
	}
	return f, nil
}

func writeRunnerFrame(ctx context.Context, conn *websocket.Conn, f protocol.Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

func closeRunner(conn *websocket.Conn, code websocket.StatusCode, reason string) {
	_ = conn.Close(code, reason)
}

// newRunnerToken returns a random 32-byte runner token (base64url) and its
// idle expiry. The token is never logged and never reaches an event.
func newRunnerToken() (string, time.Time, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", time.Time{}, fmt.Errorf("httpapi: runner token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), time.Now().Add(protocol.TokenIdle), nil
}
