package discover

import (
	"context"
	"errors"
	"io"
)

// Temporary: replaced by the real implementation before the PR.

// Discover reads the cluster and drafts pipelines.
func Discover(ctx context.Context, d Deps, o Options) (Result, error) {
	return Result{}, errors.New("discover: not implemented")
}

// Render writes r as a draft owners file.
func Render(w io.Writer, r Result) error { return errors.New("discover: not implemented") }
