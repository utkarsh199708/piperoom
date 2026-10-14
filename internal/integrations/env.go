package integrations

import (
	"log/slog"
	"net/url"
	"sort"
	"strings"

	"piperoom/internal/integrations/spec"
)

// envVar is one environment variable of a category and how a View shows it.
type envVar struct {
	name   string
	field  string // form field it maps to; "" = not shown
	secret bool   // value is a secret: shown only as secrets_set (or its reference)
	dbURL  bool   // a database URL: split into host, port, database, user + password
}

// envCategories lists, per category, the variables that make it env-managed
// (integrations.md §4.4). Names match internal/config.
var envCategories = map[spec.Category][]envVar{
	spec.CategorySourceDB: {{name: "SOURCE_DB_URL", dbURL: true}, {name: "SOURCE_DB_PASSWORD", field: "password", secret: true}},
	spec.CategoryWarehouse: {{name: "WAREHOUSE_DB_URL", dbURL: true}, {name: "WAREHOUSE_DB_PASSWORD", field: "password", secret: true},
		{name: "WAREHOUSE_KIND"}},
	spec.CategoryKafka: {
		{name: "KAFKA_BROKERS", field: "brokers"}, {name: "KAFKA_SECURITY_PROTOCOL", field: "protocol"},
		{name: "KAFKA_SASL_MECHANISM", field: "mechanism"}, {name: "KAFKA_SASL_USERNAME", field: "username"},
		{name: "KAFKA_SASL_PASSWORD", field: "password", secret: true}, {name: "KAFKA_TLS_CA_FILE"},
		{name: "KAFKA_TLS_CERT_FILE"}, {name: "KAFKA_TLS_KEY_FILE"}, {name: "KAFKA_TLS_SERVER_NAME", field: "server_name"},
	},
	spec.CategoryConnect: {
		{name: "CONNECT_URL", field: "url"}, {name: "CONNECT_USERNAME", field: "username"},
		{name: "CONNECT_PASSWORD", field: "password", secret: true},
		{name: "CONNECT_TLS_CA_FILE"}, {name: "CONNECT_TLS_CERT_FILE"}, {name: "CONNECT_TLS_KEY_FILE"},
	},
	spec.CategoryRegistry: {
		{name: "SCHEMA_REGISTRY_URL", field: "url"}, {name: "SCHEMA_REGISTRY_USERNAME", field: "username"},
		{name: "SCHEMA_REGISTRY_PASSWORD", field: "password", secret: true},
		{name: "SCHEMA_REGISTRY_TLS_CA_FILE"}, {name: "SCHEMA_REGISTRY_TLS_CERT_FILE"}, {name: "SCHEMA_REGISTRY_TLS_KEY_FILE"},
	},
	spec.CategoryModel: {
		{name: "ANTHROPIC_API_KEY", field: "api_key", secret: true}, {name: "PIPEROOM_MODEL_PROVIDER"},
		{name: "VERTEX_PROJECT_ID"}, {name: "VERTEX_REGION"},
	},
	spec.CategoryChat: {{name: "SLACK_BOT_TOKEN", field: "bot_token", secret: true}, {name: "SLACK_CHANNEL_ID", field: "channel_id"}},
}

// alertEnv maps an alert source to its secret variable.
var alertEnv = map[string]string{
	"alertmanager": "ALERTMANAGER_TOKEN", "grafana": "GRAFANA_ALERT_SECRET", "pagerduty": "PAGERDUTY_ALERT_SECRET",
	"datadog": "DATADOG_ALERT_TOKEN", "generic": "GENERIC_ALERT_SECRET",
}

// EnvCategory is a category configured by environment variables.
type EnvCategory struct {
	Kind       string         // best-matching kind id
	Config     map[string]any // non-secret values, and {"$ref"} for secret variables holding a reference
	SecretsSet []string       // secret fields with a value, sorted
}

// Env is which categories and alert sources the environment configures. It
// applies to spec.DefaultWorkspace only: environment variables configure the
// one workspace of a self-hosted install.
type Env struct {
	Categories   map[spec.Category]EnvCategory
	AlertSecrets map[string]bool // source → its secret variable is set
}

// EnvFromLookup reads the environment through lookup (os.LookupEnv in the
// server). A variable counts as set when it is present and not blank.
// config.Load's dev defaults (KAFKA_BROKERS=localhost:29092, ...) are not in
// the environment, so they never make a category env-managed.
func EnvFromLookup(lookup func(string) (string, bool)) Env {
	get := func(name string) string {
		v, ok := lookup(name)
		if !ok {
			return ""
		}
		return strings.TrimSpace(v)
	}
	env := Env{Categories: map[spec.Category]EnvCategory{}, AlertSecrets: map[string]bool{}}
	for cat, vars := range envCategories {
		ec := EnvCategory{Config: map[string]any{}}
		set := false
		secrets := map[string]bool{}
		for _, ev := range vars {
			val := get(ev.name)
			if val == "" {
				continue
			}
			set = true
			switch {
			case ev.dbURL:
				dbURLView(val, ec.Config, secrets)
			case ev.secret:
				secrets[ev.field] = true
				if strings.HasPrefix(val, spec.RefPrefix) {
					ec.Config[ev.field] = map[string]any{spec.RefKey: val}
				}
			case ev.field != "":
				ec.Config[ev.field] = val
			}
		}
		if !set {
			continue
		}
		ec.SecretsSet = sortedKeys(secrets)
		ec.Kind = envKind(cat, get)
		env.Categories[cat] = ec
	}
	for src, name := range alertEnv {
		if get(name) != "" {
			env.AlertSecrets[src] = true
		}
	}
	return env
}

// dbURLView splits a database URL into non-secret fields. The password is
// only reported as set. A Snowflake DSN (no scheme) shows nothing but that a
// password may be set.
func dbURLView(raw string, cfg map[string]any, secrets map[string]bool) {
	if strings.HasPrefix(raw, spec.RefPrefix) {
		// The whole URL comes from a secret manager: its parts are not
		// known here, and parsing the reference as a URL would show the
		// provider id as the host and the secret path as the database.
		secrets["password"] = true
		return
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		secrets["password"] = true
		return
	}
	cfg["host"] = u.Hostname()
	if p := u.Port(); p != "" {
		cfg["port"] = p
	}
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		cfg["database"] = db
	}
	if u.User != nil {
		cfg["user"] = u.User.Username()
		if _, ok := u.User.Password(); ok {
			secrets["password"] = true
		}
	}
	if m := u.Query().Get("sslmode"); m != "" {
		cfg["sslmode"] = m
	}
}

func envKind(cat spec.Category, get func(string) string) string {
	switch cat {
	case spec.CategorySourceDB:
		return KindPostgresSource
	case spec.CategoryWarehouse:
		if strings.EqualFold(get("WAREHOUSE_KIND"), "snowflake") {
			return KindSnowflake
		}
		return KindWarehousePostgres
	case spec.CategoryKafka:
		if strings.Contains(get("KAFKA_BROKERS"), ".confluent.cloud") {
			return KindKafkaConfluentCloud
		}
		return KindKafka
	case spec.CategoryConnect:
		return KindConnectREST
	case spec.CategoryRegistry:
		return KindSchemaRegistry
	case spec.CategoryModel:
		return KindAnthropic
	case spec.CategoryChat:
		return KindSlack
	}
	return ""
}

// managed reports whether the environment configures cat in workspace ws.
func (e Env) managed(ws string, cat spec.Category) bool {
	if ws != spec.DefaultWorkspace {
		return false
	}
	_, ok := e.Categories[cat]
	return ok
}

func (e Env) alertManaged(ws, source string) bool {
	return ws == spec.DefaultWorkspace && e.AlertSecrets[source]
}

// LogSources writes the start-up line naming each category's source, e.g.
// "integrations: kafka from env, connect from db, source_db not configured".
// It contains no values.
func LogSources(log *slog.Logger, env Env, dbCategories []spec.Category) {
	if log == nil {
		log = slog.Default()
	}
	inDB := map[spec.Category]bool{}
	for _, c := range dbCategories {
		inDB[c] = true
	}
	var parts []string
	for _, c := range spec.Categories {
		if c == spec.CategoryAlerts || c == spec.CategorySecretManager {
			continue
		}
		switch {
		case env.managed(spec.DefaultWorkspace, c):
			parts = append(parts, string(c)+" from env")
		case inDB[c]:
			parts = append(parts, string(c)+" from db")
		default:
			parts = append(parts, string(c)+" not configured")
		}
	}
	var alerts []string
	for src := range env.AlertSecrets {
		alerts = append(alerts, src)
	}
	sort.Strings(alerts)
	log.Info("integrations: "+strings.Join(parts, ", "), "alert_sources_from_env", alerts)
}
