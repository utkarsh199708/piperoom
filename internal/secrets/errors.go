package secrets

import (
	"errors"

	"piperoom/internal/integrations/spec"
)

// Errors. Callers compare with errors.Is. No error text from this package
// ever contains a secret value.
var (
	// ErrInvalidRef: the text is not a valid secret:// reference.
	ErrInvalidRef = errors.New("secrets: invalid reference")
	// ErrUnknownProvider: no provider of that id in the caller's workspace.
	// A provider of another workspace is reported the same way, so a
	// reference cannot probe other workspaces.
	ErrUnknownProvider = errors.New("secrets: unknown secret provider")
	// ErrNotFound: the manager has no secret (or version) at that path.
	ErrNotFound = errors.New("secrets: secret not found")
	// ErrKeyNotFound: the secret has no field named by the reference's #key,
	// or it is not a JSON or key/value secret.
	ErrKeyNotFound = errors.New("secrets: key not found in secret")
	// ErrAccessDenied: the manager refused Piperoom's identity (403).
	ErrAccessDenied = errors.New("secrets: access denied by secret manager")
	// ErrLogin: Piperoom could not log in to the manager.
	ErrLogin = errors.New("secrets: login to secret manager failed")
	// ErrOutsideRoots: a files reference names a path outside the allowed roots.
	ErrOutsideRoots = errors.New("secrets: path is outside the allowed secret file roots")
	// ErrClosed: the provider or registry was closed.
	ErrClosed = errors.New("secrets: closed")
	// ErrBadProviderConfig: a provider declaration or constructor input is invalid.
	ErrBadProviderConfig = errors.New("secrets: invalid provider configuration")

	// ErrUnreachable is spec.ErrProviderUnreachable: the manager did not
	// answer (network error, timeout, 5xx). The HTTP API maps it to 502.
	ErrUnreachable = spec.ErrProviderUnreachable
	// ErrListUnsupported is spec.ErrListUnsupported: the provider (or its
	// policy) cannot list names; the UI shows a text field instead.
	ErrListUnsupported = spec.ErrListUnsupported
)
