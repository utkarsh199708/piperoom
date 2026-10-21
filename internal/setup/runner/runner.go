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
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
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

	// connectBackoffBase, connectBackoffMax and maxConnectRetries bound the
	// retries of a transient connect or handshake failure: the runner waits
	// exponentially longer (capped) and gives up after maxConnectRetries.
	connectBackoffBase = time.Second
	connectBackoffMax  = 30 * time.Second
	maxConnectRetries  = 5
)

// Errors the caller branches on.
var (
	ErrPairingRefused = errors.New("setup: pairing refused")
	ErrYesOffLoopback = errors.New("setup: --yes is refused unless the server is loopback")
	ErrFrameTooBig    = errors.New("setup: frame exceeds the 1 MiB cap")
	// ErrTokenExpired reports that the runner's token is past its idle expiry
	// (or was rejected by the server); the person must pair again with a new
	// code.
	ErrTokenExpired = errors.New("setup: runner token expired; pair again with a new code")
)

// errRedirectRefused fails a handshake that answers with a redirect, so a
// server cannot move the pairing off the origin the person confirmed.
var errRedirectRefused = errors.New("setup: refusing a redirect during the handshake")

// Options configures a Runner. ServerURL and Code are required.
type Options struct {
	ServerURL  string           // http(s)://host[:port]
	Code       string           // the pairing code
	Yes        bool             // skip the y/N confirmation; loopback only
	ProjectDir string           // "" = "."
	Label      string           // "" = defaultLabel
	Stdin      io.Reader        // nil = os.Stdin
	Stdout     io.Writer        // nil = os.Stdout; the person-facing prompt
	Log        *slog.Logger     // nil = slog.Default()
	LookupIP   LookupIPFunc     // nil = the system resolver
	Now        func() time.Time // nil = time.Now; the runner's clock (tests inject one)
	Catalogue  Catalogue        // nil = nopCatalogue (no op runs until ops lands)
	// Backoff returns how long to wait before the attempt-th retry (1-based)
	// of a transient connect or handshake failure; nil = exponential backoff
	// from connectBackoffBase, capped at connectBackoffMax.
	Backoff func(attempt int) time.Duration
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
	now        func() time.Time
	catalogue  Catalogue
	backoff    func(attempt int) time.Duration
	url        *url.URL

	connMu sync.Mutex
	conn   *websocket.Conn

	token       string    // never logged, never printed
	tokenExpiry time.Time // the token is idle-expired past this instant
	roomID      string
	negotiated  protocol.Version

	// opMu guards everything an op goroutine touches below: the duplicate
	// ledger, the cached frozen proposals, the class floors, the in-flight set
	// and the open op log.
	opMu      sync.Mutex
	started   map[stepKey]struct{}
	proposals map[string]events.ProposalCreatedPayload
	floors    map[string]string
	inflight  map[string]*inflightOp
	opLog     *opLog
	opLogDone bool // op log closed; late appends are no-ops
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
		now:        o.Now,
		catalogue:  o.Catalogue,
		backoff:    o.Backoff,
		started:    make(map[stepKey]struct{}),
		proposals:  make(map[string]events.ProposalCreatedPayload),
		floors:     make(map[string]string),
		inflight:   make(map[string]*inflightOp),
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
	if r.now == nil {
		r.now = time.Now
	}
	if r.catalogue == nil {
		r.catalogue = nopCatalogue{}
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
// server closes it, reconnecting with the token (and a Resume) while it is
// still idle-valid. It returns nil on a normal close, ErrDeclined when the
// person answered N, and ErrTokenExpired when the token expired and a new
// pairing code is required.
func (r *Runner) Run(ctx context.Context) error {
	defer r.closeOpLog()

	loopback := r.isLoopback(ctx)
	if r.yes && !loopback {
		return ErrYesOffLoopback
	}
	wsURL, err := r.wsURL(loopback)
	if err != nil {
		return err
	}

	for {
		if err := r.connectLoop(ctx, loopback, wsURL); err != nil {
			return err
		}
		err := r.serve(ctx)
		if err == nil {
			// A clean close (or the ctx ending) is the server saying goodbye;
			// the runner does not reconnect after it.
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if !reconnectable(err) {
			return err
		}
		if r.tokenExpired() {
			r.log.Info("runner token expired; a new pairing code is required", "room_id", r.roomID)
			return ErrTokenExpired
		}
		r.log.Info("connection lost; reconnecting", "room_id", r.roomID, "err", err)
	}
}

// connectLoop dials and authenticates, retrying transient dial and handshake
// failures with bounded backoff. A refused pairing, a declined confirmation,
// an expired or rejected token, a protocol refusal, a dial that resolved off
// loopback, and a ctx that ended all return immediately.
func (r *Runner) connectLoop(ctx context.Context, loopback bool, wsURL string) error {
	for attempt := 0; ; attempt++ {
		err := r.connect(ctx, loopback, wsURL)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if !connectTransient(err) {
			return err
		}
		if r.token != "" && r.tokenExpired() {
			r.log.Info("runner token expired; a new pairing code is required", "room_id", r.roomID)
			return ErrTokenExpired
		}
		if attempt >= maxConnectRetries {
			return err
		}
		d := r.backoffDelay(attempt + 1)
		r.log.Info("connect failed; retrying", "room_id", r.roomID, "attempt", attempt+1, "backoff", d, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d):
		}
	}
}

// connectTransient reports whether a connect error is a transient dial or
// handshake failure the runner should retry. Terminal errors — a refused
// pairing, a declined confirmation, an expired or rejected token, a protocol
// version or frame refusal, a dial that resolved off loopback — return false.
func connectTransient(err error) bool {
	if websocket.CloseStatus(err) == protocol.CloseProtocolVersion {
		return false
	}
	switch {
	case errors.Is(err, ErrPairingRefused),
		errors.Is(err, ErrDeclined),
		errors.Is(err, ErrTokenExpired),
		errors.Is(err, ErrDialNotLoopback),
		errors.Is(err, ErrFrameTooBig),
		errors.Is(err, errRedirectRefused),
		errors.Is(err, protocol.ErrProtocolVersion),
		errors.Is(err, protocol.ErrMalformedFrame):
		return false
	}
	return true
}

// backoffDelay returns the wait before the attempt-th retry (1-based): the
// injected Options.Backoff when set, else exponential backoff from
// connectBackoffBase, capped at connectBackoffMax.
func (r *Runner) backoffDelay(attempt int) time.Duration {
	if r.backoff != nil {
		return r.backoff(attempt)
	}
	d := connectBackoffBase
	for i := 1; i < attempt; i++ {
		if d >= connectBackoffMax {
			return connectBackoffMax
		}
		d *= 2
		if d > connectBackoffMax {
			d = connectBackoffMax
		}
	}
	return d
}

// reconnectable reports whether a serve error means the connection dropped
// (reconnect with the token) rather than a fatal protocol problem (an
// oversized or malformed frame).
func reconnectable(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, ErrFrameTooBig) || errors.Is(err, protocol.ErrMalformedFrame) {
		return false
	}
	return true
}

// connect dials the socket and authenticates: a first pairing with the code
// (pair), or a reconnect with the token followed by a Resume.
func (r *Runner) connect(ctx context.Context, loopback bool, wsURL string) error {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{HTTPClient: r.httpClient(loopback)})
	cancel()
	if err != nil {
		return fmt.Errorf("setup: dialing %s: %w", r.serverURL, err)
	}
	conn.SetReadLimit(protocol.MaxFrameBytes)
	r.setConn(conn)
	r.log.Debug("connected", "server", r.serverURL, "loopback", loopback)

	if r.token == "" {
		if err := r.pair(ctx); err != nil {
			_ = conn.Close(websocket.StatusNormalClosure, "")
			r.setConn(nil)
			return err
		}
		return nil
	}
	if err := r.resume(ctx); err != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "")
		r.setConn(nil)
		return err
	}
	return nil
}

// httpClient builds the *http.Client for the WebSocket dial. It always refuses
// redirects (a redirect could move the handshake off the origin the person
// confirmed). When loopback is true the transport's dialer also refuses any
// non-loopback remote address, pinning the connection to loopback even if the
// name re-resolves to a public address between the loopback check and the
// connect; proxy handling is disabled on loopback so the connection cannot be
// steered through a proxy either.
func (r *Runner) httpClient(loopback bool) *http.Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return r.dial(ctx, network, addr, loopback)
		},
	}
	if !loopback {
		tr.Proxy = http.ProxyFromEnvironment
	}
	return &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return errRedirectRefused
		},
	}
}

// dial opens the TCP connection for the WebSocket dial. On loopback it pins
// each connection attempt to a loopback address (see loopbackControl).
func (r *Runner) dial(ctx context.Context, network, addr string, loopback bool) (net.Conn, error) {
	d := &net.Dialer{Timeout: dialTimeout}
	if loopback {
		d.Control = loopbackControl
	}
	return d.DialContext(ctx, network, addr)
}

// setConn and currentConn guard the conn pointer: an op goroutine from a
// previous connection can still be writing while the runner reconnects, so
// every access goes through the mutex (coder/websocket itself is safe for
// concurrent use).
func (r *Runner) setConn(c *websocket.Conn) {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	r.conn = c
}

func (r *Runner) currentConn() *websocket.Conn {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	return r.conn
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
	r.applyTargets(welcome.Targets)

	ok, err := r.confirm(ctx, welcome)
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
	if err := r.storeToken(f.Token); err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "paired with %s as %q\n", r.serverURL, r.label)
	r.log.Info("paired", "room_id", r.roomID, "label", r.label, "negotiated", r.negotiated.String())
	return nil
}

// resume authenticates a reconnect with the token: it sends Pair{Token}, takes
// the fresh token the server rotates to, and then sends Resume as the first
// frame after the reconnect. The server may answer a token-Pair with a Welcome
// first (updated targets); the runner re-applies its class floors (never
// lowered) and keeps waiting for the token.
func (r *Runner) resume(ctx context.Context) error {
	cur := protocol.Current()
	if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Token:           r.token,
		ProtocolVersion: cur.Major,
		ProtocolMinor:   cur.Minor,
	}}); err != nil {
		return err
	}

	f, err := r.readUntil(ctx, tokenTimeout, func(f protocol.Frame) (bool, error) {
		switch f.Type {
		case protocol.FrameWelcome:
			r.applyTargets(f.Welcome.Targets)
			return false, nil
		case protocol.FrameToken:
			return true, nil
		}
		return false, fmt.Errorf("setup: expected token, got %q", f.Type.Short())
	})
	if err != nil {
		return tokenPhaseErr(err)
	}
	if err := r.storeToken(f.Token); err != nil {
		return err
	}

	res := r.buildResume()
	if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameResume, Resume: &res}); err != nil {
		return err
	}
	r.log.Info("reconnected", "room_id", r.roomID, "in_flight", len(res.InFlight))
	return nil
}

// tokenPhaseErr classifies an error while waiting for a fresh token on a
// reconnect. A close with CloseProtocolVersion is a version refusal (the
// server moved incompatibly); any other close most likely rejected the token,
// which means a new pairing code.
func tokenPhaseErr(err error) error {
	if errors.Is(err, ErrFrameTooBig) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("setup: no token within %s", tokenTimeout)
	}
	if sc := websocket.CloseStatus(err); sc == protocol.CloseProtocolVersion {
		return pairingRefused(err)
	}
	if sc := websocket.CloseStatus(err); sc >= 0 {
		return fmt.Errorf("%w (server closed with status %d)", ErrTokenExpired, sc)
	}
	return fmt.Errorf("setup: reading token: %w", err)
}

// serve answers ping with pong, dispatches op and cancel frames, and drops
// unknown frame types. It returns when the socket closes; the conn is closed
// and cleared on the way out (op goroutines hold the Run ctx, not this one, so
// a running op survives a dropped connection and is reported in Resume).
func (r *Runner) serve(ctx context.Context) error {
	conn := r.currentConn()
	if conn == nil {
		return errors.New("setup: not connected")
	}
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "")
		r.setConn(nil)
	}()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.pingLoop(connCtx)

	for {
		f, err := r.readFrame(connCtx)
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
			if err := r.writeFrame(connCtx, protocol.Frame{Type: protocol.FramePong}); err != nil {
				return err
			}
		case protocol.FramePong:
		case protocol.FrameOp:
			go r.runOp(ctx, f.Op)
		case protocol.FrameCancel:
			r.cancelOp(f.Cancel.ID)
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
	conn := r.currentConn()
	if conn == nil {
		return protocol.Frame{}, errors.New("setup: not connected")
	}
	_, data, err := conn.Read(ctx)
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
	conn := r.currentConn()
	if conn == nil {
		return errors.New("setup: not connected")
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
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
			return fmt.Errorf("setup: server refused: %s", scrub(ce.Reason, maxPrintedFieldLen))
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
