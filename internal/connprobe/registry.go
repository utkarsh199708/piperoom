package connprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"piperoom/internal/wiring"
)

var registrySvc = httpService{
	Name:    "Schema Registry",
	URLKey:  "SCHEMA_REGISTRY_URL",
	UserKey: "SCHEMA_REGISTRY_USERNAME",
	PassKey: "SCHEMA_REGISTRY_PASSWORD",
	TLSKeys: "SCHEMA_REGISTRY_TLS_CA_FILE, SCHEMA_REGISTRY_TLS_CERT_FILE and SCHEMA_REGISTRY_TLS_KEY_FILE",
	CAKey:   "SCHEMA_REGISTRY_TLS_CA_FILE",
	Accept:  "application/vnd.schemaregistry.v1+json, application/json",
}

// compatLevels are the Schema Registry compatibility levels; anything else
// is shown as "unknown".
var compatLevels = map[string]bool{
	"BACKWARD": true, "BACKWARD_TRANSITIVE": true, "FORWARD": true, "FORWARD_TRANSITIVE": true,
	"FULL": true, "FULL_TRANSITIVE": true, "NONE": true,
}

// probeSchemaRegistry checks the Schema Registry: config, reach (GET
// /subjects), login, the pipeline topics' subjects, and their compatibility.
func probeSchemaRegistry(ctx context.Context, t Target, g *Guard) Result {
	var r Result
	var hp *httpProbe
	r.Checks = append(r.Checks, step(func() Check {
		p, bad := newHTTPProbe(registrySvc, t.Config.SchemaRegistryURL, wiring.RegistryHTTP(t.Config), g)
		if bad != nil {
			return *bad
		}
		hp = p
		return Check{ID: "config", Status: StatusOK, Detail: httpConfigDetail(p)}
	}))
	if hp == nil {
		skipRestInto(&r, "settings are incomplete", "reach", "login", "subjects", "compatibility")
		return r
	}
	defer hp.close()

	var all []string
	reach, login, _ := restReach(ctx, hp, g, "/subjects", func(b []byte) (string, bool) {
		if err := json.Unmarshal(b, &all); err != nil {
			return "", false
		}
		return fmt.Sprintf("%d subjects", len(all)), true
	})
	r.Checks = append(r.Checks, reach)
	if reach.Status == StatusFail {
		skipRestInto(&r, "Schema Registry not reachable", "login", "subjects", "compatibility")
		return r
	}
	r.Checks = append(r.Checks, login)
	if login.Status == StatusFail {
		skipRestInto(&r, "login failed", "subjects", "compatibility")
		return r
	}

	have := make(map[string]bool, len(all))
	for _, s := range all {
		have[s] = true
	}
	var found []string // value and key subjects that exist, in topic order
	topics := pipelineTopics(t)
	if len(topics) == 0 {
		r.Checks = append(r.Checks, skipped("subjects", "no pipeline topics to check"))
	} else {
		r.Checks = append(r.Checks, step(func() Check {
			var missing []string
			values, keys := 0, 0
			for _, tp := range topics {
				if v := tp + "-value"; have[v] {
					values++
					found = append(found, v)
				} else {
					missing = append(missing, v)
				}
				if k := tp + "-key"; have[k] {
					keys++
					found = append(found, k)
				}
			}
			detail := fmt.Sprintf("%d of %d value subjects, %d key subjects", values, len(topics), keys)
			if len(missing) > 0 {
				return Check{ID: "subjects", Status: StatusWarn, Detail: detail,
					Problem: "No schema subject for: " + strings.Join(missing, ", ") + ".",
					Fix:     "Check the pipeline's topic_prefix and source_tables in owners.yaml, that the connector uses the Avro converter with TopicNameStrategy, and that it has written its first change."}
			}
			return Check{ID: "subjects", Status: StatusOK, Detail: detail}
		}))
	}

	r.Checks = append(r.Checks, step(func() Check { return compatibilityCheck(ctx, hp, found) }))
	return r
}

// compatibilityCheck reads the compatibility of each subject (falling back
// to the global level when a subject has none of its own), or only the
// global level when no subject was found. NONE is a warning.
func compatibilityCheck(ctx context.Context, hp *httpProbe, subjects []string) Check {
	var problems, details, none []string
	var global string
	var globalErr string
	readGlobal := func() (string, bool) {
		if global != "" {
			return global, true
		}
		if globalErr != "" {
			return "", false
		}
		lvl, _, perr := readCompat(ctx, hp, "/config", "the global compatibility")
		if perr != "" {
			globalErr = perr
			problems = append(problems, perr)
			return "", false
		}
		global = lvl
		return lvl, true
	}
	for _, s := range subjects {
		lvl, unset, perr := readCompat(ctx, hp, "/config/"+url.PathEscape(s), "the compatibility of subject "+s)
		switch {
		case unset:
			g, ok := readGlobal()
			if !ok {
				continue
			}
			details = append(details, s+" "+g+" (global default)")
			if g == "NONE" {
				none = append(none, s)
			}
		case perr != "":
			problems = append(problems, perr)
		default:
			details = append(details, s+" "+lvl)
			if lvl == "NONE" {
				none = append(none, s)
			}
		}
	}
	if len(subjects) == 0 {
		if g, ok := readGlobal(); ok {
			details = append(details, "global "+g)
			if g == "NONE" {
				none = append(none, "the global default")
			}
		}
	}
	c := Check{ID: "compatibility", Status: StatusOK, Detail: strings.Join(details, "; ")}
	switch {
	case len(problems) > 0:
		c.Status = StatusFail
		c.Problem = upperFirst(strings.Join(problems, "; ")) + "."
		c.Fix = "Allow GET /config/{subject} and GET /config for this login (docs/pilot/permissions.md, Schema Registry read-only)."
	case len(none) > 0:
		c.Status = StatusWarn
		c.Problem = "Compatibility NONE on " + strings.Join(none, ", ") + " lets incompatible schemas through."
		c.Fix = "Have the schema-registry layer owner set BACKWARD (or stricter): PUT /config/{subject} {\"compatibility\": \"BACKWARD\"}."
	}
	return c
}

// readCompat reads one /config answer. It returns the level, or unset for a
// subject with no level of its own (404: the global level applies), or a
// fixed problem phrase.
func readCompat(ctx context.Context, hp *httpProbe, path, what string) (level string, unset bool, problem string) {
	status, b, err := hp.get(ctx, path)
	switch {
	case err != nil:
		return "", false, restErrText(err, what)
	case status == http.StatusNotFound && path != "/config":
		return "", true, ""
	case isAuthStatus(status):
		return "", false, fmt.Sprintf("the login may not read %s (HTTP %d)", what, status)
	case status != http.StatusOK:
		return "", false, fmt.Sprintf("reading %s failed (HTTP %d)", what, status)
	}
	var cfg struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
		Compatibility      string `json:"compatibility"`
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return "", false, what + " is not Schema Registry JSON"
	}
	lvl := strings.ToUpper(strings.TrimSpace(cfg.CompatibilityLevel))
	if lvl == "" {
		lvl = strings.ToUpper(strings.TrimSpace(cfg.Compatibility))
	}
	if !compatLevels[lvl] {
		lvl = "unknown"
	}
	return lvl, false, ""
}
