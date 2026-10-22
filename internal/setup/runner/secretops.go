package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// The runner's secret operations (docs/design/setup-agent.md §2.4):
//
//   - secret_set is a T1 op the runner itself executes: it asks the person on
//     the runner's terminal for the value and stores it. The value is never
//     in Params, a result, a summary, Data, a receipt or a log line.
//   - FrameSecret is the UI path: the driver typed the value into Piperoom and
//     the server forwards it in one frame. The runner stores it and prints the
//     name only.
//   - FrameRegisterCredentials asks for the read credentials generated during
//     setup. The runner prints the server origin, the pipeline and the names,
//     waits for "y", and sends one Credentials frame (§3.7).

// OpSecretSet is the built-in setup op that stores one named secret. It is
// not in the ops catalogue on purpose: its value comes from the runner's
// terminal, never from request params.
const OpSecretSet = "secret_set"

// opRegisterCredentials names the runner-side handler in a refusal; the
// server's own frame type is register_credentials.
const opRegisterCredentials = "register_credentials"

// isBuiltinOp reports whether the runner executes the op itself instead of
// handing it to the catalogue.
func isBuiltinOp(op string) bool { return op == OpSecretSet }

// floorOf returns the op's tier floor: the catalogue's, or T1 for secret_set,
// whose value the runner reads itself.
func (r *Runner) floorOf(op string) (string, bool) {
	if tier, ok := r.catalogue.Floor(op); ok {
		return tier, true
	}
	if isBuiltinOp(op) {
		return protocol.TierT1, true
	}
	return "", false
}

// executeBuiltin runs an op the runner owns. Only secret_set is built in.
func (r *Runner) executeBuiltin(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	switch req.Op {
	case OpSecretSet:
		return r.execSecretSet(ctx, req)
	}
	return protocol.OpResult{ID: req.ID, OK: false, Summary: "setup: no such built-in op"}, errors.New("setup: no such built-in op")
}

// secretSetParams is secret_set's whole request: a name. A value is never in
// params.
type secretSetParams struct {
	Name string `json:"name"`
}

// execSecretSet asks the person for the value on the runner's terminal and
// stores it. The summary names the secret and only the secret; a refusal
// never quotes the value.
func (r *Runner) execSecretSet(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error) {
	var p secretSetParams
	if err := decodeSecretParams(req.Params, &p); err != nil {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused secret_set: " + err.Error()}, err
	}
	if err := ValidateSecretName(p.Name); err != nil {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused secret_set: " + err.Error()}, err
	}
	st, err := r.secretStore()
	if err != nil {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused secret_set: the secret store is unavailable"}, err
	}
	value, err := r.promptSecret(ctx, p.Name)
	if err != nil {
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused secret_set: " + err.Error()}, err
	}
	if err := st.Set(p.Name, value); err != nil {
		// Set's errors name the secret or the bounds, never the value.
		return protocol.OpResult{ID: req.ID, OK: false, Summary: "refused secret_set " + p.Name + ": " + err.Error()}, err
	}
	r.log.Info("secret stored", "name", p.Name)
	return protocol.OpResult{
		ID:      req.ID,
		OK:      true,
		Summary: "stored secret " + p.Name,
		Receipt: events.SetupReceipt{Target: "secret:" + p.Name, At: r.now()},
	}, nil
}

// decodeSecretParams decodes secret_set's params strictly.
func decodeSecretParams(raw json.RawMessage, dst *secretSetParams) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("setup: invalid secret_set params: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return errors.New("setup: invalid secret_set params: trailing JSON")
	}
	if dst.Name == "" {
		return errors.New("setup: invalid secret_set params: name is required")
	}
	return nil
}

// promptSecret prints the secret's name and reads one line from the runner's
// terminal. The value is never written to stdout, a log or an error; the
// terminal's own echo is the person's screen, not a log. The read is
// serialized with every other terminal prompt (stdinMu) and returns when a
// line arrives or ctx ends.
func (r *Runner) promptSecret(ctx context.Context, name string) (string, error) {
	r.stdinMu.Lock()
	defer r.stdinMu.Unlock()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if r.stdinReader == nil {
		r.stdinReader = bufio.NewReader(r.stdin)
	}
	fmt.Fprintf(r.stdout, "secret %s: value (stored in the runner's secret store, never printed or logged)\nvalue: ", scrub(name, MaxSecretNameBytes))

	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := r.stdinReader.ReadString('\n')
		ch <- result{line, err}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-ch:
		if res.err != nil && !errors.Is(res.err, io.EOF) {
			return "", fmt.Errorf("setup: reading the secret value: %w", res.err)
		}
		value := strings.TrimSuffix(strings.TrimSuffix(res.line, "\n"), "\r")
		if value == "" && res.err != nil {
			return "", errors.New("setup: no secret value was entered")
		}
		return value, nil
	}
}

// storeSecretFrame handles FrameSecret, the UI path: the driver typed the
// value into Piperoom and the server forwards it once. The runner stores it
// and prints the name only; there is no answer frame (the server emitted
// setup.secret_set with the name and actor).
func (r *Runner) storeSecretFrame(secret *protocol.SecretValue) {
	if secret == nil {
		return
	}
	name := scrub(secret.Name, MaxSecretNameBytes)
	st, err := r.secretStore()
	if err == nil {
		err = st.Set(secret.Name, secret.Value)
	}
	if err != nil {
		// Neither Set's nor OpenStore's errors quote a value.
		r.log.Warn("secret refused", "name", name, "err", err)
		fmt.Fprintf(r.stdout, "secret %s refused: %v\n", name, err)
		return
	}
	r.log.Info("secret stored", "name", name)
	fmt.Fprintf(r.stdout, "secret %s stored\n", name)
}

// registerCredentials handles FrameRegisterCredentials: it prints the server
// origin, the pipeline and the secret names, waits for a "y" on the runner's
// terminal, and only then sends the one Credentials frame. A refusal answers
// with Refusal.ID set to the request id (protocol.RegisterCredentials). The
// values are never printed or logged, and no other frame carries them.
func (r *Runner) registerCredentials(ctx context.Context, req *protocol.RegisterCredentials) {
	refuse := func() {
		ref := &protocol.Refusal{ID: req.RequestID, Op: opRegisterCredentials, Reason: protocol.RefuseCredentials}
		if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameRefused, Refused: ref}); err != nil {
			r.log.Debug("writing credentials refusal failed", "request_id", req.RequestID, "err", err)
		}
		r.log.Info("credentials refused", "request_id", req.RequestID)
	}

	if req == nil || req.RequestID == "" || len(req.Names) == 0 {
		if req != nil {
			refuse()
		}
		return
	}
	st, err := r.secretStore()
	if err != nil {
		refuse()
		return
	}

	// The server checks the names before this frame; the runner checks them
	// again and refuses the whole request when one is unknown, so a partial
	// set is never sent.
	seen := make(map[string]struct{}, len(req.Names))
	secrets := make([]protocol.Secret, 0, len(req.Names))
	for _, name := range req.Names {
		if err := ValidateSecretName(name); err != nil {
			refuse()
			return
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		value, ok := st.Get(name)
		if !ok {
			refuse()
			return
		}
		secrets = append(secrets, protocol.Secret{Name: name, Value: value})
	}
	if len(secrets) == 0 {
		refuse()
		return
	}

	fmt.Fprintf(r.stdout, "register read credentials with %s\n", r.serverURL)
	fmt.Fprintf(r.stdout, "pipeline %s\n", scrub(req.PipelineID, maxPrintedFieldLen))
	for _, s := range secrets {
		fmt.Fprintf(r.stdout, "  %s\n", s.Name)
	}
	ok, err := r.ask(ctx, "send these secrets to the server? [y/N] ")
	if err != nil || !ok {
		refuse()
		return
	}
	if err := r.writeFrame(ctx, protocol.Frame{Type: protocol.FrameCredentials, Credentials: &protocol.Credentials{
		RequestID: req.RequestID,
		Secrets:   secrets,
	}}); err != nil {
		r.log.Debug("writing credentials failed", "request_id", req.RequestID, "err", err)
		return
	}
	r.log.Info("credentials sent", "request_id", req.RequestID, "names", len(secrets))
}
