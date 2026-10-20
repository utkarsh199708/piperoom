package databricks

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
)

// maxResponseBytes caps one HTTP response from the workspace (AGENTS.md §4
// rule 7). This client reads one row, one MAX, a few key columns or at most
// maxCountBuckets counts, all far below it; cloud fetch (results staged in
// cloud storage) is switched off, so every result comes through here.
const maxResponseBytes = 4 << 20

// ErrResponseTooLarge is wrapped when a workspace response is over 4 MB.
var ErrResponseTooLarge = errors.New("databricks: response over 4 MB")

// callStatus is what the transport saw during one client call: the last
// 401, 403 or 404, and whether a body was cut. The driver turns HTTP
// failures into plain text errors, so this is how the client tells a bad
// credential or a wrong http path apart without matching on message text.
type callStatus struct {
	httpCode atomic.Int32
	oversize atomic.Bool
}

type callStatusKey struct{}

func withCallStatus(ctx context.Context, st *callStatus) context.Context {
	return context.WithValue(ctx, callStatusKey{}, st)
}

func callStatusFrom(ctx context.Context) *callStatus {
	st, _ := ctx.Value(callStatusKey{}).(*callStatus)
	return st
}

// guardTransport wraps the HTTP transport the driver uses.
type guardTransport struct {
	base http.RoundTripper
}

func (t guardTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	st := callStatusFrom(req.Context())
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		if st != nil {
			st.httpCode.Store(int32(resp.StatusCode))
		}
	}
	resp.Body = &cappedBody{rc: resp.Body, left: maxResponseBytes, st: st}
	return resp, nil
}

// cappedBody fails a read past maxResponseBytes instead of buffering it.
type cappedBody struct {
	rc   io.ReadCloser
	left int64
	st   *callStatus
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		// One more byte means the body is over the cap; exactly at the cap
		// is fine.
		var one [1]byte
		n, err := b.rc.Read(one[:])
		if n > 0 {
			if b.st != nil {
				b.st.oversize.Store(true)
			}
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.rc.Read(p)
	b.left -= int64(n)
	return n, err
}

func (b *cappedBody) Close() error { return b.rc.Close() }
