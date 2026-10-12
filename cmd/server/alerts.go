package main

import (
	"fmt"
	"strings"

	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// alertConfig builds the one alertin.Config the signed alert routes use from
// the environment config and the loaded owners file. A source with an empty
// secret stays disabled (its route answers 401). A bad PAGERDUTY_SERVICES or
// DEFAULT_ALERT_PIPELINE entry refuses start rather than silently routing
// alerts to the wrong pipeline.
func alertConfig(cfg config.Config, own *owners.Owners) (alertin.Config, error) {
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
