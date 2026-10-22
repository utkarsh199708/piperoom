//go:build integration

package testenv

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestStagingProfile checks the `staging` compose profile against a running
// stack (`devctl up --staging`): both staging services answer, the connector is
// RUNNING there under its own prefix and slot, production's connector is
// untouched, and the two registries and the Connect internal topics are
// separate. It only reads, and it skips when the profile is not up.
func TestStagingProfile(t *testing.T) {
	cfg, stg := RequireStaging(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const connector = "pg-orders-connector"

	get := func(base, path string, out any) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s%s: HTTP %d", base, path, resp.StatusCode)
		}
		return json.Unmarshal(b, out)
	}
	running := func(base string) bool {
		var st struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
			Tasks []struct {
				State string `json:"state"`
			} `json:"tasks"`
		}
		if err := get(base, "/connectors/"+connector+"/status", &st); err != nil {
			return false // not registered yet
		}
		return st.Connector.State == "RUNNING" && len(st.Tasks) > 0 && st.Tasks[0].State == "RUNNING"
	}

	// TC1: the connector is RUNNING on staging, and production is unaffected.
	WaitFor(t, 90*time.Second, func() (bool, error) { return running(stg.ConnectURL), nil })
	if !running(cfg.ConnectURL) {
		t.Error("production connector is not RUNNING")
	}
	var stgCfg, prodCfg map[string]string
	if err := get(stg.ConnectURL, "/connectors/"+connector+"/config", &stgCfg); err != nil {
		t.Fatal(err)
	}
	if err := get(cfg.ConnectURL, "/connectors/"+connector+"/config", &prodCfg); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"topic.prefix", "slot.name", "publication.name"} {
		if stgCfg[k] == "" || stgCfg[k] == prodCfg[k] {
			t.Errorf("%s: staging %q, production %q: must differ", k, stgCfg[k], prodCfg[k])
		}
	}

	// Separate registries: the staging connector's subject is only in staging's.
	const subject = "pgstg.public.orders-value"
	WaitFor(t, 60*time.Second, func() (bool, error) {
		var subs []string
		if err := get(stg.SchemaRegistryURL, "/subjects", &subs); err != nil {
			return false, err
		}
		return slices.Contains(subs, subject), nil
	})
	var prodSubs []string
	if err := get(cfg.SchemaRegistryURL, "/subjects", &prodSubs); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(prodSubs, subject) {
		t.Errorf("production registry has the staging subject %s", subject)
	}

	// Separate internal topics on the shared Kafka.
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(cfg.KafkaBrokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	topics, err := kadm.NewClient(cl).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"_connect_configs_staging", "_connect_offsets_staging", "_connect_status_staging", "_schemas_staging", "pgstg.public.orders"} {
		if !topics.Has(name) {
			t.Errorf("topic %s missing", name)
		}
	}
}
