package kafka

import "context"

// boundCtx runs fn and returns when it returns or when ctx ends, whichever
// comes first.
//
// kgo does not honour the request context while it sets up a connection: the
// ApiVersions (and SASL) exchange on a new connection is bounded only by
// RequestTimeoutOverhead (10 s), so a broker that accepts TCP but never
// answers holds a kadm call that long whatever the caller's deadline. On
// ctx's end fn's goroutine is left to finish on kgo's own timeout; the
// buffered channel lets it exit without a reader, so it never leaks past that.
func boundCtx[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn(ctx)
		ch <- result{v, err}
	}()
	select {
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
