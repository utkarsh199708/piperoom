package secrets

import (
	"context"
	"sync"
	"time"
)

// minRenewDelay keeps a lease that is (nearly) expired from spinning.
const minRenewDelay = 10 * time.Millisecond

// Renewer keeps a lease alive: it calls renew at two thirds of the time left
// until each expiry. When renew fails, or returns an expiry that is not in
// the future (the lease hit its max TTL), it calls lost once and stops; the
// owner then fetches new credentials. It stops when ctx ends or Close is
// called, and Close waits for the goroutine, so it never leaks.
type Renewer struct {
	renew func(ctx context.Context) (time.Time, error)
	lost  func(err error)
	now   func() time.Time

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// NewRenewer returns a stopped Renewer. renew must honour its context; lost
// may be nil.
func NewRenewer(renew func(ctx context.Context) (expiresAt time.Time, err error), lost func(err error)) *Renewer {
	return &Renewer{renew: renew, lost: lost, now: time.Now}
}

// errLeaseEnded is passed to lost when a renewal returns no future expiry.
var errLeaseEnded = leaseEnded{}

type leaseEnded struct{}

func (leaseEnded) Error() string { return "secrets: lease reached its maximum TTL" }

// Start begins renewing a lease that expires at expiresAt. Calling Start
// twice has no effect.
func (r *Renewer) Start(ctx context.Context, expiresAt time.Time) {
	r.once.Do(func() {
		ctx, r.cancel = context.WithCancel(ctx)
		r.done = make(chan struct{})
		go r.loop(ctx, expiresAt)
	})
}

// Close stops renewing and waits for the renewal loop to end. It is safe to
// call before Start, more than once, and from inside lost (which may still
// be running when Close returns).
func (r *Renewer) Close() {
	r.once.Do(func() {}) // a later Start becomes a no-op
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
}

func (r *Renewer) loop(ctx context.Context, expiresAt time.Time) {
	var lostErr error
	defer func() {
		// done closes first, so lost may call Close (or code that does).
		close(r.done)
		if lostErr != nil && r.lost != nil {
			r.lost(lostErr)
		}
	}()
	for {
		delay := r.delay(expiresAt)
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		next, err := r.renew(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil && !next.After(r.now()) {
			err = errLeaseEnded
		}
		if err != nil {
			lostErr = err
			return
		}
		expiresAt = next
	}
}

// delay is two thirds of the time left.
func (r *Renewer) delay(expiresAt time.Time) time.Duration {
	d := expiresAt.Sub(r.now()) * 2 / 3
	if d < minRenewDelay {
		return minRenewDelay
	}
	return d
}
