package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// alertConfig builds the one alertin.Config the signed alert routes use from
// the environment config and the loaded owners file. A source with an empty
// secret stays disabled (its route answers 401). A bad PAGERDUTY_SERVICES or
// DEFAULT_ALERT_PIPELINE entry refuses start rather than silently routing
// alerts to the wrong pipeline.
func alertConfig(cfg config.Config, own *owners.Owners) (alertin.Config, error) {
	if err := checkOwnersIDs(own); err != nil {
		return alertin.Config{}, err
	}
	services, err := parsePagerDutyServices(cfg.PagerDutyServices, own)
	if err != nil {
		return alertin.Config{}, err
	}
	def := strings.TrimSpace(cfg.DefaultAlertPipeline)
	if def != "" {
		if err := checkPipeline(own, def); err != nil {
			return alertin.Config{}, fmt.Errorf("server: DEFAULT_ALERT_PIPELINE: %w", err)
		}
	}
	return alertin.Config{
		AlertmanagerToken: cfg.AlertmanagerToken,
		GrafanaSecret:     cfg.GrafanaAlertSecret,
		PagerDutySecret:   cfg.PagerDutyAlertSecret,
		DatadogToken:      cfg.DatadogAlertToken,
		GenericSecret:     cfg.GenericAlertSecret,
		Owners:            own,
		PagerDutyServices: services,
		DefaultPipeline:   def,
	}, nil
}

// parsePagerDutyServices parses "SVCID=pipeline,SVCID2=pipeline2". Every entry
// must have a service ID and a pipeline that owners.yaml defines, and a service
// may appear once. An empty string is an empty map.
func parsePagerDutyServices(s string, own *owners.Owners) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, entry := range strings.Split(s, ",") {
		svc, pipeline, ok := strings.Cut(strings.TrimSpace(entry), "=")
		svc, pipeline = strings.TrimSpace(svc), strings.TrimSpace(pipeline)
		if !ok || svc == "" || pipeline == "" {
			return nil, fmt.Errorf("server: PAGERDUTY_SERVICES entry %q must look like SERVICE_ID=pipeline", strings.TrimSpace(entry))
		}
		if err := checkPipeline(own, pipeline); err != nil {
			return nil, fmt.Errorf("server: PAGERDUTY_SERVICES service %s: %w", svc, err)
		}
		if _, dup := out[svc]; dup {
			return nil, fmt.Errorf("server: PAGERDUTY_SERVICES lists service %s twice", svc)
		}
		out[svc] = pipeline
	}
	return out, nil
}

// checkOwnersIDs refuses start when an owners.yaml pipeline id is one alertin
// would silently ignore (alertin.ValidPipelineID), so alerts for it would not
// be routed there.
func checkOwnersIDs(own *owners.Owners) error {
	if own == nil {
		return nil
	}
	for _, p := range own.Pipelines {
		if !alertin.ValidPipelineID(p.ID) {
			return fmt.Errorf("server: owners pipeline id %q is not a valid alert pipeline id (letters, digits, _ . -; at most 64), so signed alerts could not be routed to it", p.ID)
		}
	}
	return nil
}

// checkPipeline fails when own does not define the pipeline.
func checkPipeline(own *owners.Owners, id string) error {
	if own == nil {
		return fmt.Errorf("no owners config to check pipeline %q against", id)
	}
	if _, ok := own.Pipeline(id); !ok {
		return fmt.Errorf("pipeline %q is not in the owners file", id)
	}
	return nil
}

// enabledAlertSources lists the sources with a secret, comma separated, or
// "none". It is for the start-up log line and never includes a secret.
func enabledAlertSources(c alertin.Config) string {
	var on []string
	for _, src := range alertin.Sources {
		if c.Enabled(src) {
			on = append(on, string(src))
		}
	}
	if len(on) == 0 {
		return "none"
	}
	return strings.Join(on, ",")
}

// changeWebhook builds POST /api/changes/webhook (X4) over the room store's
// change log. It is mounted even without CHANGES_WEBHOOK_SECRET, so a sender
// gets 401 rather than 404; only whether it is enabled is logged.
func changeWebhook(cfg config.Config, own *owners.Owners, st any, log *slog.Logger) http.Handler {
	cw, _ := st.(clients.ChangeWriter)
	h := alertin.NewChangeWebhook(alertin.ChangeWebhookConfig{Secret: cfg.ChangesWebhookSecret, Owners: own, Writer: cw, Log: log})
	log.Info("server: change webhook", "enabled", h.Enabled())
	return h
}
