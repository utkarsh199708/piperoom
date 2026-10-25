package wiring

import (
	"fmt"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/workerlogs"
	"piperoom/internal/config"
)

// openWorkerLogs builds the Loki client of connect_worker_logs (Q3c) from the
// WORKER_LOGS_* settings; consumer_logs (H2) derives its per-pipeline client
// from the same one (consumerReaders), so both tools share one URL, token,
// user and tenant. A nil client with a nil error means no Loki is configured.
// TLS stays netsec's default (system roots): there is no WORKER_LOGS_TLS_*
// setting. Nothing is dialled here, so a bad setting fails before any stack
// is reached; the error never carries the token. extra is for tests (a CA
// for an httptest Loki), after the options the settings give.
func openWorkerLogs(cfg config.Config, extra ...workerlogs.Option) (*workerlogs.Client, error) {
	wl, err := config.ParseWorkerLogs(cfg)
	if err != nil {
		return nil, err
	}
	if !wl.Configured() {
		return nil, nil
	}
	var opts []workerlogs.Option
	if wl.User != "" {
		opts = append(opts, workerlogs.WithBasicAuthUser(wl.User))
	}
	if wl.OrgID != "" {
		opts = append(opts, workerlogs.WithOrgID(wl.OrgID))
	}
	opts = append(opts, extra...)
	c, err := workerlogs.New(wl.URL, wl.Selector, netsec.Secret(wl.Token), opts...)
	if err != nil {
		return nil, fmt.Errorf("wiring: worker logs (WORKER_LOGS_*): %w", err)
	}
	return c, nil
}
