// Package runner is the setup runner (cmd/setup): it pairs with a Piperoom
// server over one WebSocket to /api/runners/ws, describes itself in Hello,
// and serves the server's ping and op frames. It owns the runner side of the
// wire contract in internal/setup/protocol.
//
// Invariants, each a test: the pairing code and the session token are never
// logged or printed; --yes skips the confirmation only on loopback; the
// transport is TLS (wss) unless loopback; frames are capped at
// protocol.MaxFrameBytes in both directions.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/setup/protocol"
)

const (
	wsPath         = "/api/runners/ws"
	dialTimeout    = 15 * time.Second
	welcomeTimeout = 30 * time.Second
	tokenTimeout   = 30 * time.Second
	// defaultLabel is a name, never the raw hostname.
	defaultLabel = "piperoom-runner"
	// catalogueVersion is "0" until the ops catalogue lands (a later wave).
	catalogueVersion = "0"
)

// Errors the caller branches on.
var (
	ErrPairingRefused = errors.New("setup: pairing refused")
	ErrYesOffLoopback = errors.New("setup: --yes is refused unless the server is loopback")
	ErrFrameTooBig    = errors.New("setup: frame exceeds the 1 MiB cap")
)

// Options configures a Runner. ServerURL and Code are required.
type Options struct {
	ServerURL  string       // http(s)://host[:port]
	Code       string       // the pairing code
	Yes        bool         // skip the y/N confirmation; loopback only
	ProjectDir string       // "" = "."
	Label      string       // "" = defaultLabel
	Stdin      io.Reader    // nil = os.Stdin
	Stdout     io.Writer    // nil = os.Stdout; the person-facing prompt
	Log        *slog.Logger // nil = slog.Default()
	LookupIP   LookupIPFunc // nil = the system resolver
}

// Runner is one paired (or pairing) setup runner.
type Runner struct {
	serverURL  string
	code       string
	yes        bool
	projectDir string
	label      string
	stdin      io.Reader
	stdout     io.Writer
	log        *slog.Logger
	lookupIP   LookupIPFunc
	url        *url.URL

	conn       *websocket.Conn
	token      string // never logged, never printed
	roomID     string
	negotiated protocol.Version
}

// New validates the options and builds a Runner.
func New(o Options) (*Runner, error) {
	r := &Runner{
		serverURL:  strings.TrimSpace(o.ServerURL),
		code:       strings.TrimSpace(o.Code),
		yes:        o.Yes,
		projectDir: orDefault(o.ProjectDir, "."),
		label:      orDefault(o.Label, defaultLabel),
		stdin:      o.Stdin,
		stdout:     o.Stdout,
		log:        o.Log,
		lookupIP:   o.LookupIP,
	}
	if r.stdin == nil {
		r.stdin = os.Stdin
	}
	if r.stdout == nil {
		r.stdout = os.Stdout
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.lookupIP == nil {
		r.lookupIP = defaultLookupIP
	}
	if r.serverURL == "" {
		return nil, errors.New("setup: --server (or PIPEROOM_SETUP_SERVER_URL) is required")
	}
	u, err := url.Parse(r.serverURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("setup: --server must be an http(s)://host URL")
	}
	if u.User != nil {
		return nil, errors.New("setup: --server must not carry user info")
	}
	r.url = u
	if err := r.checkCode(); err != nil {
		return nil, err
	}
	return r, nil
}

// Run pairs the runner and then serves the socket until ctx ends or the
// server closes it. It returns nil on a normal close, ErrDeclined when the
// person answered N.
func (r *Runner) Run(ctx context.Context) error {
	loopback := r.isLoopback(ctx)
	if r.yes && !loopback {
		return ErrYesOffLoopback
	}
	wsURL, err := r.wsURL(loopback)
	if err != nil {
		return err
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	cancel()
	if err != nil {
		return fmt.Errorf("setup: dialing %s: %w", r.serverURL, err)
	}
	r.conn = conn
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	conn.SetReadLimit(protocol.MaxFrameBytes)
	r.log.Debug("connected", "server", r.serverURL, "loopback", loopback)

	if err := r.pair(ctx); err != nil {
		return err
	}
	return r.serve(ctx)
}

// pair sends Pair, confirms the Welcome on the person's terminal, sends
// Hello, and stores the Token the server hands back.
func (r *Runner) pair(ctx context.Context) error {
	cur := protocol.Current()
	if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Code:            r.code,
		ProtocolVersion: cur.Major,
		ProtocolMinor:   cur.Minor,
	}}); err != nil {
		return err
	}

	f, err := r.readUntil(ctx, welcomeTimeout, func(f protocol.Frame) (bool, error) {
		if f.Type != protocol.FrameWelcome {
			return false, fmt.Errorf("setup: expected welcome, got %q", f.Type.Short())
		}
		return true, nil
	})
	if err != nil {
		return phaseErr("welcome", welcomeTimeout, true, err)
	}
	welcome := f.Welcome

	// The compatibility check runs before anything is printed or confirmed.
	if err := protocol.RunnerAccepts(cur, welcome.Version()); err != nil {
		r.log.Warn("server protocol version refused", "server", welcome.Version().String(), "runner", cur.String())
		_ = r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameRefused, Refused: &protocol.Refusal{
			ID: "", Op: "pair", Reason: protocol.RefuseProtocolVersion,
		}})
		return err
	}
	r.negotiated = protocol.Negotiate(cur, welcome.Version())
	r.roomID = welcome.RoomID

	ok, err := r.confirm(welcome)
	if err != nil {
		return err
	}
	if !ok {
		return ErrDeclined
	}

	if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameHello, Hello: &protocol.Hello{
		OS:               runtime.GOOS,
		Arch:             runtime.GOARCH,
		Label:            r.label,
		Tools:            map[string]string{}, // PATH detection lands with the ops catalogue
		ProjectDir:       r.projectDir,
		CatalogueVersion: catalogueVersion,
		ProtocolVersion:  cur.Major,
		ProtocolMinor:    cur.Minor,
	}}); err != nil {
		return err
	}

	f, err = r.readUntil(ctx, tokenTimeout, func(f protocol.Frame) (bool, error) {
		if f.Type != protocol.FrameToken {
			return false, fmt.Errorf("setup: expected token, got %q", f.Type.Short())
		}
		return true, nil
	})
	if err != nil {
		return phaseErr("token", tokenTimeout, false, err)
	}
	r.token = f.Token.Token
	if r.token == "" {
		return errors.New("setup: empty token")
	}
	fmt.Fprintf(r.stdout, "paired with %s as %q\n", r.serverURL, r.label)
	r.log.Info("paired", "room_id", r.roomID, "label", r.label, "negotiated", r.negotiated.String())
	return nil
}

// serve answers ping with pong and drops unknown frame types; op and
// credential frames are handled in later waves.
func (r *Runner) serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.pingLoop(ctx)

	for {
		f, err := r.readFrame(ctx)
		if err != nil {
			if errors.Is(err, ErrFrameTooBig) {
				return err
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			if sc := websocket.CloseStatus(err); sc == websocket.StatusNormalClosure || sc == websocket.StatusGoingAway {
				return nil
			}
			return fmt.Errorf("setup: reading frame: %w", err)
		}
		if err := f.Check(); err != nil {
			if errors.Is(err, protocol.ErrUnknownFrame) {
				r.log.Debug("dropping unknown frame", "type", f.Type.Short())
				continue
			}
			return fmt.Errorf("setup: malformed frame: %w", err)
		}
		switch f.Type {
		case protocol.FramePing:
			if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FramePong}); err != nil {
				return err
			}
		case protocol.FramePong:
		default:
			r.log.Debug("frame not handled yet", "type", f.Type.Short())
		}
	}
}

// pingLoop sends a ping every protocol.PingInterval until ctx ends, so the
// server does not mark the runner lost (protocol.LostAfter).
func (r *Runner) pingLoop(ctx context.Context) {
	t := time.NewTicker(protocol.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FramePing}); err != nil {
				r.log.Debug("ping failed", "err", err)
				return
			}
		}
	}
}

func (r *Runner) readFrame(ctx context.Context) (protocol.Frame, error) {
	_, data, err := r.conn.Read(ctx)
	if err != nil {
		if errors.Is(err, websocket.ErrMessageTooBig) || websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
			return protocol.Frame{}, fmt.Errorf("%w: server frame exceeds %d bytes", ErrFrameTooBig, protocol.MaxFrameBytes)
		}
		return protocol.Frame{}, err
	}
	var f protocol.Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return protocol.Frame{}, fmt.Errorf("setup: decoding frame: %w", err)
	}
	return f, nil
}

func (r *Runner) writeFrame(ctx context.Context, f protocol.Frame) error {
	raw, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("setup: encoding frame: %w", err)
	}
	if len(raw) > protocol.MaxFrameBytes {
		return fmt.Errorf("%w: %s is %d bytes", ErrFrameTooBig, f.Type.Short(), len(raw))
	}
	if err := r.conn.Write(ctx, websocket.MessageText, raw); err != nil {
		return fmt.Errorf("setup: writing %s: %w", f.Type.Short(), err)
	}
	return nil
}

// readUntil reads frames until fn accepts one or the deadline passes,
// answering ping with pong and dropping unknown frame types.
func (r *Runner) readUntil(ctx context.Context, timeout time.Duration, fn func(protocol.Frame) (bool, error)) (protocol.Frame, error) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		f, err := r.readFrame(rctx)
		if err != nil {
			return protocol.Frame{}, err
		}
		if err := f.Check(); err != nil {
			if errors.Is(err, protocol.ErrUnknownFrame) {
				r.log.Debug("dropping unknown frame", "type", f.Type.Short())
				continue
			}
			return protocol.Frame{}, fmt.Errorf("setup: malformed frame: %w", err)
		}
		switch f.Type {
		case protocol.FramePing:
			if err := r.writeFrame(rctx, protocol.Frame{Type: protocol.FramePong}); err != nil {
				return protocol.Frame{}, err
			}
			continue
		case protocol.FramePong:
			continue
		}
		done, err := fn(f)
		if err != nil {
			return protocol.Frame{}, err
		}
		if done {
			return f, nil
		}
		r.log.Debug("ignoring frame", "type", f.Type.Short())
	}
}

// phaseErr classifies an error while waiting for the welcome or the token;
// refuse maps a close before Welcome to ErrPairingRefused.
func phaseErr(what string, timeout time.Duration, refuse bool, err error) error {
	if errors.Is(err, ErrFrameTooBig) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("setup: no %s within %s", what, timeout)
	}
	if websocket.CloseStatus(err) >= 0 {
		if refuse {
			return pairingRefused(err)
		}
		return fmt.Errorf("setup: server closed before %s: %w", what, err)
	}
	return fmt.Errorf("setup: reading %s: %w", what, err)
}

// pairingRefused builds the error for a server that closed before Welcome. A
// close with protocol.CloseProtocolVersion names both versions in its reason,
// which the runner prints; any other close reports the status code only (the
// reason is untrusted server text).
func pairingRefused(err error) error {
	if sc := websocket.CloseStatus(err); sc == protocol.CloseProtocolVersion {
		var ce websocket.CloseError
		if errors.As(err, &ce) && ce.Reason != "" {
			return fmt.Errorf("setup: server refused: %s", ce.Reason)
		}
		return fmt.Errorf("setup: server refused the protocol version (close %d)", sc)
	}
	if sc := websocket.CloseStatus(err); sc >= 0 {
		return fmt.Errorf("%w (close status %d)", ErrPairingRefused, sc)
	}
	return fmt.Errorf("%w: %v", ErrPairingRefused, err)
}

// checkCode refuses a missing, short or malformed code; the error never
// quotes the code (it is a credential).
func (r *Runner) checkCode() error {
	if r.code == "" {
		return errors.New("setup: --code (or PIPEROOM_SETUP_CODE) is required")
	}
	if len(r.code) != protocol.CodeLength {
		return fmt.Errorf("setup: pairing code must be %d characters", protocol.CodeLength)
	}
	for i := 0; i < len(r.code); i++ {
		if !strings.ContainsRune(protocol.CodeAlphabet, rune(r.code[i])) {
			return errors.New("setup: pairing code has a character outside the code alphabet")
		}
	}
	return nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
