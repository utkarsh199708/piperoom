package clients_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// reconcilerOnly implements Reconciler and nothing else: the reader is
// optional, so a client gains it without any other interface changing.
type reconcilerOnly struct{}

func (reconcilerOnly) CountBetween(context.Context, string, string, time.Time, time.Time) (int64, error) {
	return 0, nil
}

func (reconcilerOnly) SampleKeys(context.Context, string, string, string, time.Time, int) ([]string, error) {
	return nil, nil
}

func (reconcilerOnly) RowTimes(context.Context, string, string, []string, []string) (map[string]map[string]time.Time, error) {
	return nil, nil
}

var _ clients.Reconciler = reconcilerOnly{}

// The caps are part of the contract (X1-0): the watcher and the clients
// both code against these numbers.
func TestReconcileCaps(t *testing.T) {
	if clients.MaxReconcileSample != 200 || clients.MaxReconcileKeys != 1000 {
		t.Fatalf("caps = %d, %d; want 200, 1000", clients.MaxReconcileSample, clients.MaxReconcileKeys)
	}
}
