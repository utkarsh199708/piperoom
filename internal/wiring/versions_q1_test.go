package wiring

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
)

// lifecycleSpy is the writing spy plus what the real Connect client grew in
// the Q wave: the offsets, version and plugin reads (Q1c, Q5a) and the
// lifecycle writes (Q5a), which record like the spy's other writes.
type lifecycleSpy struct{ *writeSpy }

var (
	_ connectLifecycleReads          = lifecycleSpy{}
	_ connect.ConnectLifecycleWriter = lifecycleSpy{}
)

func (lifecycleSpy) SourceOffsets(context.Context, string) ([]connect.SourceOffset, error) {
	return []connect.SourceOffset{{Partition: map[string]any{"server": "orders"}, Offset: map[string]any{"lsn": 42}}}, nil
}
func (lifecycleSpy) ServerInfo(context.Context) (connect.ServerInfo, error) {
	return connect.ServerInfo{Version: "3.6.1"}, nil
}
func (lifecycleSpy) Loggers(context.Context) ([]connect.LoggerLevel, error) {
	return []connect.LoggerLevel{{Logger: "root", Level: "INFO"}}, nil
}
func (lifecycleSpy) Plugins(context.Context) ([]connect.Plugin, error) {
	return []connect.Plugin{{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source", Version: "2.5.1.Final"}}, nil
}
func (s lifecycleSpy) CreateConnector(context.Context, string, map[string]string) error {
	s.writes = append(s.writes, "CreateConnector")
	return nil
}
func (s lifecycleSpy) DeleteConnector(context.Context, string) error {
	s.writes = append(s.writes, "DeleteConnector")
	return nil
}
func (s lifecycleSpy) StopConnector(context.Context, string) error {
	s.writes = append(s.writes, "StopConnector")
	return nil
}
func (s lifecycleSpy) ResetSourceOffsets(context.Context, string) error {
	s.writes = append(s.writes, "ResetSourceOffsets")
	return nil
}
func (s lifecycleSpy) AlterSourceOffsets(context.Context, string, []connect.SourceOffset) error {
	s.writes = append(s.writes, "AlterSourceOffsets")
	return nil
}

// taskLifecycleSpy adds the worker-task read, so every wrapper shape is hit.
type taskLifecycleSpy struct{ lifecycleSpy }

func (taskLifecycleSpy) WorkerTasks(context.Context) (map[string][]clients.TaskAssignment, error) {
	return nil, nil
}

// plainLifecycleSpy has the Q reads but neither the config validation pair
// nor the worker-task read.
type plainLifecycleSpy struct {
	clients.ConnectClient
	connectLifecycleReads
}

// TestReadConnectKeepsLifecycleReads (Q1d): the read-only Connect value keeps
// the offsets, version and plugin reads exactly when the client has them, in
// every wrapper shape, never the lifecycle writes, and its writes still
// refuse. The actions' read-only value (readOnlyActionConnect) stays without
// them: no action needs them with write mode off.
func TestReadConnectKeepsLifecycleReads(t *testing.T) {
	ctx := context.Background()
	sp := &writeSpy{}
	life := lifecycleSpy{sp}
	for name, c := range map[string]clients.ConnectClient{
		"validate":       life,
		"validate+tasks": taskLifecycleSpy{life},
		"plain":          plainLifecycleSpy{ConnectClient: readOnlyConnect{r: sp}, connectLifecycleReads: life},
		"tasks":          plainTaskLifecycleSpy{plainLifecycleSpy{ConnectClient: readOnlyConnect{r: sp}, connectLifecycleReads: life}},
	} {
		t.Run(name, func(t *testing.T) {
			c := readConnect(&Wired{Connect: c})
			l, ok := c.(connectLifecycleReads)
			if !ok {
				t.Fatalf("readConnect = %T, which hides the Q reads", c)
			}
			if info, err := l.ServerInfo(ctx); err != nil || info.Version != "3.6.1" {
				t.Errorf("ServerInfo = %+v, %v", info, err)
			}
			if pl, err := l.Plugins(ctx); err != nil || len(pl) != 1 {
				t.Errorf("Plugins = %v, %v", pl, err)
			}
			if offs, err := l.SourceOffsets(ctx, "c"); err != nil || len(offs) != 1 {
				t.Errorf("SourceOffsets = %v, %v", offs, err)
			}
			// connect_worker_info's logger levels (Q3a) read through it.
			if lv, err := l.Loggers(ctx); err != nil || len(lv) != 1 {
				t.Errorf("Loggers = %v, %v", lv, err)
			}
			if _, ok := c.(connect.ConnectLifecycleWriter); ok {
				t.Fatalf("readConnect = %T, which carries the lifecycle writes", c)
			}
			if err := c.RestartConnector(ctx, "c"); !errors.Is(err, ErrReadOnly) {
				t.Errorf("RestartConnector = %v, want ErrReadOnly", err)
			}
		})
	}
	if len(sp.writes) != 0 {
		t.Fatalf("a read-only value wrote: %v", sp.writes)
	}
	if _, ok := readConnect(&Wired{Connect: sp}).(connectLifecycleReads); ok {
		t.Fatal("readConnect grew the Q reads on a client without them")
	}
	if _, ok := readOnlyActionConnect(&Wired{Connect: life}).(connectLifecycleReads); ok {
		t.Fatal("the actions' read-only value grew the Q reads")
	}
}

// plainTaskLifecycleSpy has the Q reads and the worker-task read only.
type plainTaskLifecycleSpy struct{ plainLifecycleSpy }

func (plainTaskLifecycleSpy) WorkerTasks(context.Context) (map[string][]clients.TaskAssignment, error) {
	return nil, nil
}
