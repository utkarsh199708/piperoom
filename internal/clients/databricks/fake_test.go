package databricks

import (
	"context"
	"sync"
)

// fakeRunner answers queries in order from results; it records every query.
// With block set it waits for ctx to end and returns ctx.Err(), so timeout
// and cancellation paths can be tested without a network. httpCode, when
// set, is stored in the call's status as the transport would.
type fakeRunner struct {
	mu      sync.Mutex
	results []fakeResult
	block   bool
	queries []query
}

type fakeResult struct {
	res      result
	err      error
	httpCode int32
	oversize bool
}

func (f *fakeRunner) run(ctx context.Context, q query) (result, error) {
	f.mu.Lock()
	f.queries = append(f.queries, q)
	var r fakeResult
	if len(f.results) > 0 {
		r, f.results = f.results[0], f.results[1:]
	}
	block := f.block
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return result{}, ctx.Err()
	}
	if st := callStatusFrom(ctx); st != nil {
		if r.httpCode != 0 {
			st.httpCode.Store(r.httpCode)
		}
		if r.oversize {
			st.oversize.Store(true)
		}
	}
	if r.err != nil {
		return result{}, r.err
	}
	// A real runner never returns more rows than asked for.
	if len(r.res.Rows) > q.MaxRows {
		r.res.Rows = r.res.Rows[:q.MaxRows]
	}
	return r.res, nil
}

func (f *fakeRunner) calls() []query {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]query(nil), f.queries...)
}

var testCfg = Config{
	Host: "dbc-1234abcd-5678.cloud.databricks.com", Port: 443, HTTPPath: "/sql/1.0/warehouses/abc123",
	Catalog: "main", Token: testToken,
}

func newFake(results ...fakeResult) (*DB, *fakeRunner) {
	f := &fakeRunner{results: results}
	return newWithRunner(testCfg, f), f
}

func ok(fields []field, rows ...[]any) fakeResult {
	return fakeResult{res: result{Fields: fields, Rows: rows}}
}

func fail(err error) fakeResult { return fakeResult{err: err} }

// execErr imitates the driver's execution error: it carries a SQLSTATE.
type execErr struct {
	msg, state string
}

func (e execErr) Error() string    { return e.msg }
func (e execErr) SqlState() string { return e.state }
