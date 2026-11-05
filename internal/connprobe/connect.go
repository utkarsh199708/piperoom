package connprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/wiring"
)

var connectSvc = httpService{
	Name:    "Kafka Connect",
	URLKey:  "CONNECT_URL",
	UserKey: "CONNECT_USERNAME",
	PassKey: "CONNECT_PASSWORD",
	TLSKeys: "CONNECT_TLS_CA_FILE, CONNECT_TLS_CERT_FILE and CONNECT_TLS_KEY_FILE",
	CAKey:   "CONNECT_TLS_CA_FILE",
	Accept:  "application/json",
}

// connectorStates are the states the Connect REST API reports. Anything else
// is shown as "unknown" so no free server text reaches the result.
var connectorStates = map[string]bool{
	"RUNNING": true, "PAUSED": true, "FAILED": true, "UNASSIGNED": true, "STOPPED": true, "RESTARTING": true,
}

func knownState(s string) string {
	if connectorStates[s] {
		return s
	}
	return "unknown"
}

// probeConnect checks Kafka Connect: config, reach (GET /connectors), login,
// each pipeline connector's status, and that its config is readable. It
// never puts a config value in the result.
func probeConnect(ctx context.Context, t Target, g *Guard) Result {
	var r Result
	var hp *httpProbe
	r.Checks = append(r.Checks, step(func() Check {
		p, bad := newHTTPProbe(connectSvc, t.Config.ConnectURL, wiring.ConnectHTTP(t.Config), g)
		if bad != nil {
			return *bad
		}
		hp = p
		return Check{ID: "config", Status: StatusOK, Detail: httpConfigDetail(p)}
	}))
	if hp == nil {
		skipRestInto(&r, "settings are incomplete", "reach", "login", "connectors", "config_read")
		return r
	}
	defer hp.close()

	reach, login, _ := restReach(ctx, hp, g, "/connectors", func(b []byte) (string, bool) {
		var names []string
		if err := json.Unmarshal(b, &names); err != nil {
			return "", false
		}
		return fmt.Sprintf("%d connectors", len(names)), true
	})
	r.Checks = append(r.Checks, reach)
	if reach.Status == StatusFail {
		skipRestInto(&r, "Kafka Connect not reachable", "login", "connectors", "config_read")
		return r
	}
	r.Checks = append(r.Checks, login)
	if login.Status == StatusFail {
		skipRestInto(&r, "login failed", "connectors", "config_read")
		return r
	}

	names := pipelineConnectors(t)
	if len(names) == 0 {
		skipRestInto(&r, "no pipeline connectors to check", "connectors", "config_read")
		return r
	}
	var found []string
	r.Checks = append(r.Checks, step(func() Check {
		var details, problems, missing []string
		for _, n := range names {
			path := "/connectors/" + url.PathEscape(n) + "/status"
			status, b, err := hp.get(ctx, path)
			switch {
			case err != nil:
				problems = append(problems, restErrText(err, "status of connector "+n))
			case status == http.StatusNotFound:
				missing = append(missing, n)
			case isAuthStatus(status):
				problems = append(problems, fmt.Sprintf("the login may not read the status of connector %s (HTTP %d)", n, status))
			case status != http.StatusOK:
				problems = append(problems, fmt.Sprintf("reading the status of connector %s failed (HTTP %d)", n, status))
			default:
				d, ok := connectorStatusDetail(n, b)
				if !ok {
					problems = append(problems, "the status of connector "+n+" is not Connect REST JSON")
					continue
				}
				found = append(found, n)
				details = append(details, d)
			}
		}
		c := Check{ID: "connectors", Status: StatusOK, Detail: strings.Join(details, "; ")}
		switch {
		case len(missing) > 0:
			c.Status = StatusFail
			c.Problem = "Connector not found on this Kafka Connect cluster: " + strings.Join(missing, ", ") + "."
			if len(problems) > 0 {
				c.Problem += " Also: " + strings.Join(problems, "; ") + "."
			}
			c.Fix = "Check `connectors:` of the pipeline in owners.yaml against GET /connectors, and that CONNECT_URL is the cluster that runs it."
		case len(problems) > 0:
			c.Status = StatusFail
			c.Problem = upperFirst(strings.Join(problems, "; ")) + "."
			c.Fix = "Allow GET /connectors/{name}/status for this login (docs/pilot/permissions.md, Kafka Connect read calls)."
		}
		return c
	}))

	if len(found) == 0 {
		r.Checks = append(r.Checks, skipped("config_read", "no pipeline connector found"))
		return r
	}
	r.Checks = append(r.Checks, step(func() Check {
		var problems []string
		for _, n := range found {
			path := "/connectors/" + url.PathEscape(n) + "/config"
			status, b, err := hp.get(ctx, path)
			switch {
			case err != nil:
				problems = append(problems, restErrText(err, "config of connector "+n))
			case status != http.StatusOK:
				problems = append(problems, fmt.Sprintf("the config of connector %s cannot be read (HTTP %d)", n, status))
			default:
				// Only the shape is checked; no key or value is kept.
				var m map[string]json.RawMessage
				if err := json.Unmarshal(b, &m); err != nil {
					problems = append(problems, "the config of connector "+n+" is not Connect REST JSON")
				}
			}
		}
		if len(problems) > 0 {
			return Check{ID: "config_read", Status: StatusFail,
				Problem: upperFirst(strings.Join(problems, "; ")) + ".",
				Fix:     "Allow GET /connectors/{name}/config for this login (docs/pilot/permissions.md, Kafka Connect read calls)."}
		}
		return Check{ID: "config_read", Status: StatusOK, Detail: fmt.Sprintf("config readable for %d connectors", len(found))}
	}))
	return r
}

// connectorStatusDetail turns a status answer into "name STATE, k/n tasks
// RUNNING", with states limited to the known set.
func connectorStatusDetail(name string, b []byte) (string, bool) {
	var st struct {
		Connector *struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(b, &st); err != nil || st.Connector == nil {
		return "", false
	}
	running := 0
	for _, tk := range st.Tasks {
		if tk.State == "RUNNING" {
			running++
		}
	}
	return fmt.Sprintf("%s %s, %d/%d tasks RUNNING", name, knownState(st.Connector.State), running, len(st.Tasks)), true
}

// httpConfigDetail says which scheme and login the probe uses, no values.
func httpConfigDetail(h *httpProbe) string {
	login := "no login"
	if h.hasLogin() {
		login = "basic auth"
	}
	return h.base.Scheme + ", " + login
}

// restErrText describes a transport error on one object in fixed words.
func restErrText(err error, what string) string {
	switch {
	case errors.Is(err, errBodyTooLarge):
		return "the " + what + " is larger than 4 MB"
	case errors.Is(err, netsec.ErrBadPath):
		return "the " + what + " cannot be requested: the name is empty or a dot segment"
	case errors.Is(err, ErrBlockedAddress):
		return "reading the " + what + " was refused by the address guard"
	case isTimeout(err):
		return "reading the " + what + " timed out after 10 s"
	default:
		return "reading the " + what + " failed (network error)"
	}
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
