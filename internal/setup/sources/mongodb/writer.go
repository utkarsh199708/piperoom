package mongodb

import (
	"context"
	"fmt"

	"piperoom/internal/clients"
)

// Writer is the MongoDB clients.SetupSourceWriter of a setup room, wrapped
// around the writer that executes (internal/clients/mongodb.SetupWriter on the
// runner, or the runner's remote client once it carries MongoDB ops). It adds
// the checks the contract cannot express per source before any I/O: rule 6
// names within 64 bytes, no system database, a non-reserved user, the canary
// nonce's shape, and CreatePublication and AlterSystem as
// clients.ErrNotSupported (change streams need no publication, and no MongoDB
// setting is on the setup allow-list). No method reads or returns a password.
type Writer struct{ w clients.SetupSourceWriter }

var _ clients.SetupSourceWriter = (*Writer)(nil)

// NewWriter wraps the source setup writer; a nil writer is ErrNoWriter.
func NewWriter(w clients.SetupSourceWriter) (*Writer, error) {
	if w == nil {
		return nil, ErrNoWriter
	}
	return &Writer{w: w}, nil
}

func (w *Writer) inner() (clients.SetupSourceWriter, error) {
	if w == nil || w.w == nil {
		return nil, ErrNoWriter
	}
	return w.w, nil
}

// CreatePublication has no MongoDB equivalent: a change stream sees every
// collection the user may watch. clients.ErrNotSupported before any I/O.
func (w *Writer) CreatePublication(_ context.Context, name string, _ []string) error {
	return fmt.Errorf("mongodb writer: create publication %q: %w", clipName(name), clients.ErrNotSupported)
}

// CreateCanaryTable creates the ladder's canary collection ("db.collection";
// Driver.CanaryNamespace is its usual name).
func (w *Writer) CreateCanaryTable(ctx context.Context, namespace string) error {
	if _, _, err := splitCollection(namespace); err != nil {
		return fmt.Errorf("mongodb writer: create canary collection: %w", err)
	}
	in, err := w.inner()
	if err != nil {
		return err
	}
	if err := in.CreateCanaryTable(ctx, namespace); err != nil {
		return fmt.Errorf("mongodb writer: create canary collection %s: %w", namespace, err)
	}
	return nil
}

// InsertCanary inserts one canary document whose _id is the ladder's nonce.
func (w *Writer) InsertCanary(ctx context.Context, namespace, nonce string) error {
	if _, _, err := splitCollection(namespace); err != nil {
		return fmt.Errorf("mongodb writer: insert canary: %w", err)
	}
	if !canaryNonceRe.MatchString(nonce) {
		return fmt.Errorf("%w: the canary nonce is 16 random bytes as 32 lower-case hex characters", ErrParams)
	}
	in, err := w.inner()
	if err != nil {
		return err
	}
	if err := in.InsertCanary(ctx, namespace, nonce); err != nil {
		return fmt.Errorf("mongodb writer: insert canary into %s: %w", namespace, err)
	}
	return nil
}

// CreateReadRole creates the capture user in admin with a role that reads the
// oplog and finds and watches collections, and returns only the secret store's
// name for its generated password.
func (w *Writer) CreateReadRole(ctx context.Context, user string, collections []string) (string, error) {
	if err := checkUser(user); err != nil {
		return "", fmt.Errorf("mongodb writer: create capture user: %w", err)
	}
	if err := checkCollections(collections); err != nil {
		return "", fmt.Errorf("mongodb writer: create capture user %s: %w", user, err)
	}
	in, err := w.inner()
	if err != nil {
		return "", err
	}
	name, err := in.CreateReadRole(ctx, user, collections)
	if err != nil {
		return "", fmt.Errorf("mongodb writer: create capture user %s: %w", user, err)
	}
	if !secretNameRe.MatchString(name) {
		return "", fmt.Errorf("mongodb writer: create capture user %s: the writer returned no usable secret name: %w", user, ErrParams)
	}
	return name, nil
}

// AlterSystem has no MongoDB equivalent on the setup allow-list:
// clients.ErrNotSupported before any I/O, whatever the wrapped writer is.
func (w *Writer) AlterSystem(_ context.Context, name, _ string) (bool, error) {
	return false, fmt.Errorf("mongodb writer: alter system %q: %w", clipName(name), clients.ErrNotSupported)
}
