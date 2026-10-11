//go:build integration

package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
)

// repoRoot walks up from the package dir until it finds go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from cwd")
		}
		dir = parent
	}
}

// connectClient is a minimal Connect REST client for the integration test.
type connectClient struct {
	base string
	hc   *http.Client
}

func newConnectClient(base string) *connectClient {
	return &connectClient{base: base, hc: &http.Client{Timeout: 5 * time.Second}}
}

func (c *connectClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("connect %s: %s", path, resp.Status)
	}
	return json.Unmarshal(body, out)
}

func (c *connectClient) ListConnectors(ctx context.Context) ([]string, error) {
	var names []string
	if err := c.get(ctx, "/connectors", &names); err != nil {
		return nil, err
	}
	return names, nil
}

func (c *connectClient) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	var raw struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Connector struct {
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
		} `json:"connector"`
		Tasks []struct {
			ID       int    `json:"id"`
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
			Trace    string `json:"trace"`
		} `json:"tasks"`
	}
	if err := c.get(ctx, "/connectors/"+name+"/status", &raw); err != nil {
		return clients.ConnectorStatus{}, err
	}
	st := clients.ConnectorStatus{
		Name:     raw.Name,
		Type:     raw.Type,
		State:    raw.Connector.State,
		WorkerID: raw.Connector.WorkerID,
	}
	for _, tk := range raw.Tasks {
		st.Tasks = append(st.Tasks, clients.TaskStatus{ID: tk.ID, State: tk.State, WorkerID: tk.WorkerID, Trace: tk.Trace})
	}
	return st, nil
}

func (c *connectClient) Config(ctx context.Context, name string) (map[string]string, error) {
	var m map[string]string
	if err := c.get(ctx, "/connectors/"+name+"/config", &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *connectClient) RestartTask(context.Context, string, int) error { return nil }
func (c *connectClient) RestartConnector(context.Context, string) error { return nil }

// kafkaAdmin is a minimal Kafka admin for the integration test.
type kafkaAdmin struct {
	adm *kadm.Client
}

func newKafkaAdmin(t *testing.T, brokers string) *kafkaAdmin {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return &kafkaAdmin{adm: kadm.NewClient(cl)}
}

func (k *kafkaAdmin) Topics(ctx context.Context) ([]string, error) {
	details, err := k.adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(details))
	for name := range details {
		names = append(names, name)
	}
	return names, nil
}

func (k *kafkaAdmin) TopicOffsets(ctx context.Context, topic string) ([]clients.PartitionOffsets, error) {
	ends, err := k.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	starts, err := k.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	var out []clients.PartitionOffsets
	for p, e := range ends[topic] {
		s, _ := starts.Lookup(topic, p)
		out = append(out, clients.PartitionOffsets{Partition: p, Start: s.Offset, End: e.Offset})
	}
	return out, nil
}

func (k *kafkaAdmin) ConsumerGroups(ctx context.Context) ([]string, error) {
	groups, err := k.adm.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	return names, nil
}

func (k *kafkaAdmin) GroupLag(ctx context.Context, group string) (clients.GroupLag, error) {
	lags, err := k.adm.Lag(ctx, group)
	if err != nil {
		return clients.GroupLag{}, err
	}
	dgl, ok := lags[group]
	if !ok {
		return clients.GroupLag{}, fmt.Errorf("group %s: %w", group, clients.ErrNotFound)
	}
	if err := dgl.Error(); err != nil {
		return clients.GroupLag{}, err
	}
	out := clients.GroupLag{
		Group:    group,
		State:    dgl.State,
		Members:  len(dgl.Members),
		TotalLag: dgl.Lag.Total(),
	}
	for _, m := range dgl.Lag.Sorted() {
		out.Partitions = append(out.Partitions, clients.PartitionLag{
			Topic:     m.Topic,
			Partition: m.Partition,
			Committed: m.Commit.At,
			End:       m.End.Offset,
			Lag:       m.Lag,
		})
	}
	return out, nil
}

func (k *kafkaAdmin) FindByKey(context.Context, string, string, time.Time, int) ([]clients.RecordRef, error) {
	return nil, nil // not exercised by Build
}

func TestBuildOrdersIntegration(t *testing.T) {
	cfg := testenv.Require(t)

	o, err := owners.Load(filepath.Join(repoRoot(t), "dev", "owners.yaml"))
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src, err := postgres.New(ctx, cfg.SourceDBURL)
	if err != nil {
		t.Fatalf("source postgres unreachable: %v", err)
	}
	defer src.Close()
	wh, err := warehouse.New(ctx, cfg.WarehouseDBURL)
	if err != nil {
		t.Fatalf("warehouse unreachable: %v", err)
	}
	defer wh.Close()

	b := NewBuilder(o, newConnectClient(cfg.ConnectURL), newKafkaAdmin(t, cfg.KafkaBrokers), src, wh)

	g, err := b.Build(ctx, "orders")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if len(g.Nodes) < 5 {
		t.Fatalf("nodes = %d, want >= 5: %+v", len(g.Nodes), g.Nodes)
	}
	for _, n := range g.Nodes {
		if n.Health == HealthUnknown {
			t.Fatalf("unexpected unknown node: %+v", n)
		}
	}
	// Topic derivation must find pg.public.orders from the connector config.
	found := false
	for _, n := range g.Nodes {
		if n.Kind == KindTopic && n.Name == "pg.public.orders" {
			found = true
		}
	}
	if !found {
		t.Fatalf("topic pg.public.orders missing: %+v", g.Nodes)
	}
	// The source node must resolve the connector's slot (debezium_orders).
	for _, n := range g.Nodes {
		if n.Kind == KindSourceTable && (n.Facts["slot"] == "missing" || n.Facts["slot"] == "unknown") {
			t.Fatalf("source node failed to resolve slot: %+v", n)
		}
	}
}
