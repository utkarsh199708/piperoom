package setuptools

import (
	"context"
	"encoding/json"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// fakeRunner records every request and answers with a fixed result or error.
type fakeRunner struct {
	mu    sync.Mutex
	reqs  []protocol.OpRequest
	rooms []string
	res   protocol.OpResult
	err   error
	// exec, when set, answers instead of res/err (a real catalogue).
	exec func(ctx context.Context, req protocol.OpRequest) (protocol.OpResult, error)
}

func (f *fakeRunner) Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.rooms = append(f.rooms, roomID)
	f.mu.Unlock()
	if f.exec != nil {
		return f.exec(ctx, req)
	}
	res := f.res
	res.ID = req.ID
	return res, f.err
}

func (f *fakeRunner) requests() []protocol.OpRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.OpRequest(nil), f.reqs...)
}

// recordingSink records every event the agent would append.
type recordingSink struct {
	mu   sync.Mutex
	evs  []events.Type
	pays []any
	err  error
}

func (s *recordingSink) Emit(_ context.Context, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.evs = append(s.evs, typ)
	s.pays = append(s.pays, payload)
	return nil
}

func okResult(summary string, data any) protocol.OpResult {
	raw, _ := json.Marshal(data)
	return protocol.OpResult{OK: true, Summary: summary, Data: raw,
		Receipt: events.SetupReceipt{Kind: "file", Target: "notes.txt", OutMAC: "mac"}}
}
