package room

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// Setup secrets and registered read credentials (U1-5d; docs/design/
// setup-agent.md §2.4, §3.7; docs/api.md "Setup rooms").
//
//   - POST /api/rooms/{id}/setup/secret forwards one value the driver typed
//     in the UI to the paired runner in one secret frame and stores it
//     nowhere: the room log records the name only (setup.secret_set).
//   - POST /api/rooms/{id}/setup/credentials asks the runner for the read
//     credentials it generated (register_credentials). The runner answers
//     with one Credentials frame after the person's confirmation; the server
//     accepts it only while that request is pending for the room, once, and
//     with exactly the requested names, then writes each value through an
//     os.Root at PIPEROOM_SECRETS_DIR as <pipeline>/<name>, 0600, to a
//     temporary file renamed into place. A symlink or reparse point in the
//     path is refused, never followed, and the values reach no event, log,
//     action result or prompt: the room log gets
//     setup.credentials_registered with the names only.
//
// The registration is pipeline_register's source of `file:` references
// (U1-14): <PIPEROOM_SECRETS_DIR>/<pipeline>/<name> is the path a
// `connections:` entry names.

// Setup secret bounds (docs/api.md: name ^[a-zA-Z_][a-zA-Z0-9_]*$, value at
// least 6 bytes so literal redaction cannot mangle ordinary text, at most
// 4 KiB).
const (
	minSetupSecretBytes = 6
	maxSetupSecretBytes = 4 << 10
)

// setupSecretNameRe is protocol.Secret's name rule plus a length cap. A name
// is never a path segment ("/", "\" and "." are impossible), so it is also a
// file name under PIPEROOM_SECRETS_DIR, and it is checked before any path is
// built; the cap keeps it well under any filesystem's NAME_MAX and out of
// oversized events and frames.
var setupSecretNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)

// secretsPipelineID reports whether the pipeline id is usable as a path
// segment under PIPEROOM_SECRETS_DIR. pipelineIDRe alone admits "." and "..":
// os.Root refuses ".." but opens "." as the secrets root itself, which would
// write the room's files at the top level, where a file named like another
// pipeline's id blocks that pipeline's directory for good.
func secretsPipelineID(id string) bool {
	return pipelineIDRe.MatchString(id) && strings.Trim(id, ".") != ""
}

// setupOpRegisterCredentials is the op name a credentials refusal carries
// (the runner's own refusal uses the same name).
const setupOpRegisterCredentials = "register_credentials"

// pendingCredentials is the room's one outstanding RegisterCredentials
// request: the request id the runner answers with and the exact set of names
// the answer must carry.
type pendingCredentials struct {
	requestID string
	names     []string
}

// cloudSetupSecretMessage is the refusal of a cloud setup credential's name
// on the room's secret route (docs/api.md POST /api/rooms/{id}/setup/secret;
// docs/design/setup-managed.md §2).
const cloudSetupSecretMessage = "enter cloud credentials on the runner: ccloud_*, aiven_token and aws_* are never sent through the room"

// isCloudSetupSecretName reports whether name is a managed-cloud setup
// credential (ccloud_*, aiven_token, aws_*; case-insensitive prefix match),
// the same rule as runner.IsCloudSetupSecret, which the server does not
// link. Such a value is typed on the runner's terminal only: the server
// never forwards it, never records its name (setup.secret_set) and never
// asks the runner to register it.
func isCloudSetupSecretName(name string) bool {
	n := strings.ToLower(name)
	return strings.HasPrefix(n, "ccloud_") || strings.HasPrefix(n, "aws_") || n == "aiven_token"
}

// checkSetupSecretNameValue validates one secret before anything touches the
// network or the disk. The messages never echo the value.
func checkSetupSecretNameValue(name, value string) error {
	if !setupSecretNameRe.MatchString(name) {
		return validation("name must match ^[a-zA-Z_][a-zA-Z0-9_]*$")
	}
	if isCloudSetupSecretName(name) {
		return validation(cloudSetupSecretMessage)
	}
	if len(value) < minSetupSecretBytes {
		return validation(fmt.Sprintf("value must be at least %d bytes", minSetupSecretBytes))
	}
	if len(value) > maxSetupSecretBytes {
		return validation(fmt.Sprintf("value must be at most %d bytes", maxSetupSecretBytes))
	}
	return nil
}

// SetSetupSecret is POST /api/rooms/{id}/setup/secret: the driver typed a
// value in the UI. The value travels to the paired runner in one secret
// frame and is stored nowhere on the server; the room emits setup.secret_set
// with the name and actor only. A room without a paired runner is 409
// no_runner and the event is not written, so the log never claims a value
// reached a runner that was not there.
func (s *Service) SetSetupSecret(ctx context.Context, user User, roomID, name, value string) error {
	if err := checkSetupSecretNameValue(name, value); err != nil {
		return err
	}
	// The driver's right to touch the room is settled before the frame is
	// written; the write itself is a network call and does not run under the
	// room lock, so a stalled runner cannot hold the room's reads up.
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return err
	}
	s.unlockRoom(st)
	f := protocol.Frame{Type: protocol.FrameSecret, Secret: &protocol.SecretValue{
		Secret: protocol.Secret{Name: name, Value: value},
	}}
	if err := s.runners.WriteToRoom(ctx, roomID, f); err != nil {
		if errors.Is(err, protocol.ErrNoRunner) {
			return noRunner()
		}
		return fmt.Errorf("room: forward secret to the runner: %w", err)
	}
	// Re-locked and re-verified: the room may have closed or changed drivers
	// while the frame was in flight. The runner already holds the value either
	// way; the event is the record of the click.
	st, err = s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return err
	}
	defer s.unlockRoom(st)
	actor := s.humanActor(user)
	if _, err := s.emitLocked(ctx, st, roomID, events.SetupSecretSet, actor, events.SetupSecretSetPayload{
		Name: name, Actor: actor,
	}); err != nil {
		return err
	}
	s.touchDriverLease(ctx, st, roomID)
	return nil
}

// RegisterSetupCredentials is POST /api/rooms/{id}/setup/credentials: the
// driver asks the paired runner to register the read credentials it generated.
// The request is one per room (409 already_decided while one is pending) and
// carries the room's pipeline id, never one the runner could name, plus the
// secret names the room recorded from setup.secret_set. Nothing is stored on
// the server by this call.
func (s *Service) RegisterSetupCredentials(ctx context.Context, user User, roomID string) error {
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return err
	}
	pipelineID := st.snap.PipelineID
	names := append([]string{}, st.snap.Setup.secretNames...)
	// No runner is 409 no_runner whatever else the request would say: the
	// click cannot reach anything without one.
	if !s.runners.ConnectedForRoom(roomID) {
		s.unlockRoom(st)
		return noRunner()
	}
	s.unlockRoom(st)
	// The pipeline id becomes <dir>/<pipeline>: validate it before any path
	// exists. The room validated it at create; this is the second line.
	if !secretsPipelineID(pipelineID) {
		return validation("the room's pipeline id is not a valid pipeline id")
	}
	if len(names) == 0 {
		return validation("the room has no recorded secret names to register: set a secret first (POST /api/rooms/{id}/setup/secret)")
	}
	for _, name := range names {
		if !setupSecretNameRe.MatchString(name) {
			return validation("a recorded secret name is not a valid secret name")
		}
		if isCloudSetupSecretName(name) {
			// The projection drops such names (setup_snapshot.go); this is
			// the second line: a cloud setup credential is never requested.
			return validation(cloudSetupSecretMessage)
		}
	}
	pending := pendingCredentials{requestID: NewID("cred_"), names: names}
	if err := s.addCredentialRequest(roomID, pending); err != nil {
		return err
	}
	// The frame write is a network call: it does not run under the room lock
	// (the pending request is what stops a second click, not the lock).
	f := protocol.Frame{Type: protocol.FrameRegisterCredentials, RegisterCredentials: &protocol.RegisterCredentials{
		RequestID:  pending.requestID,
		PipelineID: pipelineID,
		Names:      names,
	}}
	if err := s.runners.WriteToRoom(ctx, roomID, f); err != nil {
		s.dropCredentialRequest(roomID, pending.requestID)
		if errors.Is(err, protocol.ErrNoRunner) {
			return noRunner()
		}
		return fmt.Errorf("room: ask the runner for read credentials: %w", err)
	}
	// Re-locked only to renew the lease: the frame is out either way.
	if st, err = s.lockSetupRoom(ctx, user, roomID); err == nil {
		s.touchDriverLease(ctx, st, roomID)
		s.unlockRoom(st)
	}
	return nil
}

// RunnerCredentials handles the runner's Credentials frame (the only frame
// that carries a secret value). The frame must come from the room's current
// attachment; it is accepted only while a RegisterCredentials with the same
// request id is pending for the room, once per request, and when its names are
// exactly the requested set. Everything else — and any failure to write the
// files safely — is setup.runner_refused {reason: "credentials"}: no value is
// written, logged or emitted.
func (s *Service) RunnerCredentials(ctx context.Context, tenant, roomID, creatorID, token string, creds protocol.Credentials) {
	if !s.runners.Current(tenant, roomID, creatorID, token) {
		return // a superseded attachment's frame is not this room's answer
	}
	if err := s.acceptCredentials(ctx, roomID, creds); err != nil {
		// The runner answers once per request: when its answer is refused,
		// the pending request it answered is released (matched by id, so a
		// replayed or foreign frame releases nothing) and the driver may ask
		// again.
		s.dropCredentialRequest(roomID, creds.RequestID)
		if errors.Is(err, errCredentialsEmitFailed) {
			// The files are on disk; only the record is missing. A refusal
			// event here would claim the opposite of what happened.
			s.log.Error("room: credentials registered but the event was not recorded", "room", roomID, "request_id", safeRequestID(creds.RequestID), "err", err)
			return
		}
		s.log.Warn("room: credentials frame refused", "room", roomID, "request_id", safeRequestID(creds.RequestID), "reason", err)
		s.emitCredentialsRefusal(ctx, roomID)
	}
}

// errCredentialsEmitFailed marks the one refusal path where the values were
// already written: emitting setup.credentials_registered failed after the
// files landed. The caller must not record it as a runner refusal.
var errCredentialsEmitFailed = errors.New("room: setup.credentials_registered was not recorded")

// acceptCredentials validates and stores one Credentials frame. The pending
// request is consumed when the request id matches, so a second frame with the
// same id is refused even when the first one was itself refused for its names.
func (s *Service) acceptCredentials(ctx context.Context, roomID string, creds protocol.Credentials) error {
	// The pipeline id is the room's, never the frame's: protocol.Credentials
	// has no pipeline field, and an unknown one a peer adds is ignored.
	snap, err := s.GetRoom(ctx, roomID)
	if err != nil {
		return fmt.Errorf("the room is unavailable: %w", err)
	}
	pipelineID := snap.PipelineID
	// The names and the pipeline id are checked before any path is built.
	if !secretsPipelineID(pipelineID) {
		return fmt.Errorf("the room's pipeline id is not a valid pipeline id")
	}
	names, ok := s.takeCredentialRequest(roomID, creds.RequestID)
	if !ok {
		return fmt.Errorf("no pending credentials request with that request id")
	}
	secrets, err := checkCredentialSet(names, creds.Secrets)
	if err != nil {
		return err
	}
	dir := strings.TrimSpace(s.setupCfg.SecretsDir)
	if dir == "" {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR is not configured")
	}
	if err := writePipelineSecrets(ctx, dir, pipelineID, secrets); err != nil {
		return err
	}
	label := ""
	if snap.Setup != nil && snap.Setup.Runner != nil {
		label = snap.Setup.Runner.Label
	}
	registered := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		registered = append(registered, secret.Name)
	}
	if err := s.EmitSetup(ctx, roomID, events.SetupCredentialsRegistered, events.SetupCredentialsRegisteredPayload{
		PipelineID:  pipelineID,
		Names:       registered,
		RunnerLabel: label,
	}); err != nil {
		return fmt.Errorf("%w: %v", errCredentialsEmitFailed, err)
	}
	return nil
}

// emitCredentialsRefusal writes setup.runner_refused for a Credentials frame
// the server refused. The room is only named; the values are not in scope.
func (s *Service) emitCredentialsRefusal(ctx context.Context, roomID string) {
	if err := s.EmitSetup(ctx, roomID, events.SetupRunnerRefused, events.SetupRunnerRefusedPayload{
		Op: setupOpRegisterCredentials, Reason: protocol.RefuseCredentials,
	}); err != nil {
		s.log.Warn("room: emit credentials refusal failed", "room", roomID, "err", err)
	}
}

// safeRequestID clips a peer-supplied request id for a log line.
func safeRequestID(id string) string {
	const max = 64
	if len(id) <= max {
		return id
	}
	return id[:max] + "…"
}

// checkCredentialSet validates the frame's secrets against the requested
// names: every value present once, exactly the requested set, each name and
// value within the secret rule. It returns the secrets in the requested
// order, which is what the event records.
func checkCredentialSet(want []string, got []protocol.Secret) ([]protocol.Secret, error) {
	if len(got) != len(want) {
		return nil, fmt.Errorf("the frame carries %d secrets, the request asked for %d", len(got), len(want))
	}
	byName := make(map[string]protocol.Secret, len(got))
	for _, secret := range got {
		if !setupSecretNameRe.MatchString(secret.Name) {
			return nil, fmt.Errorf("the frame carries a name that is not a secret name")
		}
		if _, dup := byName[secret.Name]; dup {
			return nil, fmt.Errorf("the frame carries the same name twice")
		}
		byName[secret.Name] = secret
	}
	out := make([]protocol.Secret, 0, len(want))
	for _, name := range want {
		secret, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("the frame does not carry one of the requested names")
		}
		if len(secret.Value) < minSetupSecretBytes || len(secret.Value) > maxSetupSecretBytes {
			return nil, fmt.Errorf("a value is outside the secret length bounds")
		}
		out = append(out, secret)
	}
	return out, nil
}

// ---- The per-room pending request ----

// addCredentialRequest records the room's one pending request. A second one
// while the first is pending is 409 already_decided (docs/api.md).
func (s *Service) addCredentialRequest(roomID string, p pendingCredentials) error {
	s.credMu.Lock()
	defer s.credMu.Unlock()
	if _, ok := s.credPending[roomID]; ok {
		return alreadyDecided("a credentials request is already pending for this room")
	}
	s.credPending[roomID] = p
	return nil
}

// takeCredentialRequest consumes the pending request when the id matches and
// returns the exact names it asked for. The caller holds no lock, so a slow
// write never runs under it.
func (s *Service) takeCredentialRequest(roomID, requestID string) (names []string, ok bool) {
	s.credMu.Lock()
	defer s.credMu.Unlock()
	p, found := s.credPending[roomID]
	if !found || p.requestID != requestID {
		return nil, false
	}
	delete(s.credPending, roomID)
	return append([]string{}, p.names...), true
}

// dropCredentialRequest forgets a pending request whose frame could not be
// written, so the driver may ask again.
func (s *Service) dropCredentialRequest(roomID, requestID string) {
	s.credMu.Lock()
	defer s.credMu.Unlock()
	if p, ok := s.credPending[roomID]; ok && p.requestID == requestID {
		delete(s.credPending, roomID)
	}
}

// forgetCredentialRequests drops the room's pending request when the runner
// it was sent to is lost or replaced (protocol.RegisterCredentials).
func (s *Service) forgetCredentialRequests(roomID string) {
	s.credMu.Lock()
	defer s.credMu.Unlock()
	delete(s.credPending, roomID)
}

// ---- Writing the values under PIPEROOM_SECRETS_DIR ----

// errSetupSecretPath refuses a path under PIPEROOM_SECRETS_DIR that is not a
// plain directory or regular file: a symlink, junction or other reparse point
// is never followed.
var errSetupSecretPath = errors.New("setup: credentials path is not a regular file")

// writePipelineSecrets writes one pipeline's registered credentials under
// dir, through an os.Root, as <pipeline>/<name>, mode 0600, each to a
// temporary file renamed into place. The pipeline id and every name are
// checked by the caller and again here, before any path is built. A symlink
// or reparse point anywhere on the path is refused (os.Root alone follows
// links that stay inside the root), and the opened temporary file is created
// with O_EXCL so a planted name cannot redirect the write.
func writePipelineSecrets(ctx context.Context, dir, pipelineID string, secrets []protocol.Secret) error {
	if !secretsPipelineID(pipelineID) {
		return fmt.Errorf("%w: the pipeline id is not a valid pipeline id", errSetupSecretPath)
	}
	for _, secret := range secrets {
		if !setupSecretNameRe.MatchString(secret.Name) {
			return fmt.Errorf("%w: a name is not a secret name", errSetupSecretPath)
		}
	}
	// The directory is checked (and created) at start (cmd/server); creating it
	// here too keeps an in-memory test or a standalone server working and
	// makes a first registration after a wiped volume succeed.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("room: create PIPEROOM_SECRETS_DIR: %w", err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("room: open PIPEROOM_SECRETS_DIR: %w", err)
	}
	defer root.Close()
	if err := ensurePipelineDir(root, pipelineID); err != nil {
		return err
	}
	sub, err := root.OpenRoot(pipelineID)
	if err != nil {
		return fmt.Errorf("room: open pipeline secrets dir %s: %w", pipelineID, err)
	}
	defer sub.Close()
	for _, secret := range secrets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writeOneSecret(sub, secret); err != nil {
			return fmt.Errorf("room: write secret %s: %w", secret.Name, err)
		}
	}
	return nil
}

// ensurePipelineDir creates or checks <dir>/<pipeline> through the root: an
// existing entry that is a symlink or not a directory is refused.
func ensurePipelineDir(root *os.Root, pipelineID string) error {
	fi, err := root.Lstat(pipelineID)
	if errors.Is(err, fs.ErrNotExist) {
		if err := root.Mkdir(pipelineID, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("room: create pipeline secrets dir %s: %w", pipelineID, err)
		}
		if fi, err = root.Lstat(pipelineID); err != nil {
			return fmt.Errorf("room: check pipeline secrets dir %s: %w", pipelineID, err)
		}
	}
	if err != nil {
		return fmt.Errorf("room: check pipeline secrets dir %s: %w", pipelineID, err)
	}
	if fi.Mode()&fs.ModeSymlink != 0 || !fi.IsDir() {
		return fmt.Errorf("%w: %s is not a plain directory", errSetupSecretPath, pipelineID)
	}
	return nil
}

// writeOneSecret writes one value to <pipeline>/<name> in sub. The existing
// entry is refused when it is not a regular file (a symlink or reparse point
// is never followed); the value goes to a fresh temporary file and is renamed
// over the name, so a reader never sees a partial value.
func writeOneSecret(sub *os.Root, secret protocol.Secret) error {
	if fi, err := sub.Lstat(secret.Name); err == nil {
		if fi.Mode()&fs.ModeSymlink != 0 || !fi.Mode().IsRegular() {
			return fmt.Errorf("%w: %s exists and is not a regular file", errSetupSecretPath, secret.Name)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	tmp, err := tempSecretName()
	if err != nil {
		return err
	}
	f, err := sub.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = sub.Remove(tmp)
		}
	}()
	if _, err := f.WriteString(secret.Value); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temporary file: %w", err)
	}
	// OpenFile's mode is subject to the umask; the store's files are 0600
	// whatever the operator's umask is. On Windows this only clears the
	// read-only attribute, which 0600 does not set.
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return fmt.Errorf("set temporary file mode: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	if err := sub.Rename(tmp, secret.Name); err != nil {
		return fmt.Errorf("rename temporary file into place: %w", err)
	}
	cleanup = false
	return nil
}

// tempSecretName returns an unpredictable name for the temporary file. It
// starts with a dot and holds no path separator, so it cannot collide with a
// secret name.
func tempSecretName() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("setup: temporary secret name: %w", err)
	}
	return ".tmp-" + hex.EncodeToString(b[:]), nil
}
