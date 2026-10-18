package bigquery

import (
	"context"
	"sync"

	bq "cloud.google.com/go/bigquery"
)

// fakeRunner answers queries in order from results; it records every query.
// With block set it waits for ctx to end and returns ctx.Err(), so timeout
// and cancellation paths can be tested without a network.
type fakeRunner struct {
	mu      sync.Mutex
	results []fakeResult
	block   bool
	queries []query
}

type fakeResult struct {
	res result
	err error
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

var testCfg = Config{Project: "acme-data", Location: "us", BillingProject: "acme-data", MaxBytesBilled: DefaultMaxBytesBilled}

func newFake(results ...fakeResult) (*DB, *fakeRunner) {
	f := &fakeRunner{results: results}
	return newWithRunner(testCfg, f), f
}

func ok(fields []field, rows ...[]bq.Value) fakeResult {
	return fakeResult{res: result{Fields: fields, Rows: rows}}
}

func fail(err error) fakeResult { return fakeResult{err: err} }
