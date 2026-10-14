package main

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// approveConfirmEnv and approveConfirmValue are the environment form of the
// -confirm-dev-stack flag. The value is a sentence, not "1", so a stray
// export in a shell profile or a production env file cannot match by accident.
const (
	approveConfirmEnv   = "PIPEROOM_APPROVE_ALL_DEV_STACK"
	approveConfirmValue = "yes-this-is-the-local-dev-stack"
)

// errApproveAllRefused wraps every refusal of -approve-all, so a caller (and a
// test) can tell the guard said no from any other failure.
var errApproveAllRefused = errors.New("-approve-all refused")

// checkApproveAll decides whether -approve-all may run. It allows it only when
// the caller confirmed explicitly AND every address in cfg, and in the
// pipeline's own connection block, points at this machine's dev stack. It
// fails closed: an address it cannot parse, an empty required one, a login or
// TLS setting (the dev stack has none) or a warehouse other than the dev
// Postgres all refuse. Nothing is resolved over DNS, so a hostname that merely
// points at 127.0.0.1 is refused; only "localhost" and loopback IPs pass.
//
// Errors name the setting and host, never the full address: a DSN holds a
// password.
func checkApproveAll(cfg config.Config, pipe owners.Pipeline, havePipe bool, confirmFlag bool, getenv func(string) string) error {
	if !confirmFlag && getenv(approveConfirmEnv) != approveConfirmValue {
		return fmt.Errorf("%w: confirmation missing: pass -confirm-dev-stack or set %s=%s", errApproveAllRefused, approveConfirmEnv, approveConfirmValue)
	}
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }

	for _, a := range []struct {
		name, value string
		optional    bool
	}{
		{"PIPEROOM_DB_URL", cfg.RoomDBURL, false},
		{"SOURCE_DB_URL", cfg.SourceDBURL, false},
		{"WAREHOUSE_DB_URL", cfg.WarehouseDBURL, false},
		{"KAFKA_BROKERS", cfg.KafkaBrokers, false},
		{"CONNECT_URL", cfg.ConnectURL, false},
		// "off" is stored as "": a pipeline without a Schema Registry is fine.
		{"SCHEMA_REGISTRY_URL", cfg.SchemaRegistryURL, true},
	} {
		if strings.TrimSpace(a.value) == "" {
			if !a.optional {
				add("%s is empty", a.name)
			}
			continue
		}
		if err := localAddress(a.value); err != nil {
			add("%s: %v", a.name, err)
		}
	}

	if cfg.WarehouseKind != "" && !strings.EqualFold(cfg.WarehouseKind, "postgres") {
		add("WAREHOUSE_KIND is %q: only the dev Postgres warehouse is allowed", cfg.WarehouseKind)
	}
	// The dev stack is plaintext without logins. Any of these means a managed
	// or production-like setup, whatever its address says (a tunnel to prod
	// listens on localhost too).
	if p := strings.ToUpper(strings.TrimSpace(cfg.KafkaSecurityProtocol)); p != "" && p != "PLAINTEXT" {
		add("KAFKA_SECURITY_PROTOCOL is %q: the dev stack is PLAINTEXT", cfg.KafkaSecurityProtocol)
	}
	for name, v := range map[string]string{
		"KAFKA_SASL_USERNAME":      cfg.KafkaSASLUsername,
		"KAFKA_SASL_PASSWORD":      cfg.KafkaSASLPassword,
		"SCHEMA_REGISTRY_USERNAME": cfg.SchemaRegistryUsername,
		"SCHEMA_REGISTRY_PASSWORD": cfg.SchemaRegistryPassword,
		"CONNECT_USERNAME":         cfg.ConnectUsername,
		"CONNECT_PASSWORD":         cfg.ConnectPassword,
	} {
		if v != "" {
			add("%s is set: the dev stack has no logins", name)
		}
	}
	// A pipeline with its own connections: block reads addresses from env vars
	// or files that this check cannot see, so it is never "the dev stack".
	if havePipe && len(pipe.Connections) > 0 {
		add("pipeline %q has its own connections: block, which this guard cannot verify", pipe.ID)
	}

	if len(problems) > 0 {
		return fmt.Errorf("%w: not the local dev stack:\n  - %s", errApproveAllRefused, strings.Join(problems, "\n  - "))
	}
	return nil
}

// localAddress reports nil when every host in v is localhost or a loopback IP.
// v is a URL, a libpq key=value string, or a comma-separated host:port list
// (Kafka brokers).
func localAddress(v string) error {
	hosts, err := hostsOf(strings.TrimSpace(v))
	if err != nil {
		return err
	}
	if len(hosts) == 0 {
		return errors.New("no host found")
	}
	for _, h := range hosts {
		if !isLocalHost(h) {
			return fmt.Errorf("host %q is not local", h)
		}
	}
	return nil
}

func hostsOf(v string) ([]string, error) {
	switch {
	case strings.Contains(v, "://"):
		u, err := url.Parse(v)
		if err != nil {
			// Do not echo err: *url.Error repeats the whole URL, password included.
			return nil, errors.New("address does not parse as a URL")
		}
		var hosts []string
		// pgx accepts several hosts in one authority ("a:1,b:2"); url.Parse
		// keeps them as one Host string.
		for _, hp := range strings.Split(u.Host, ",") {
			hosts = append(hosts, hostOnly(hp))
		}
		// A query host= / hostaddr= overrides the authority in pgx and libpq.
		for _, k := range []string{"host", "hostaddr"} {
			for _, qv := range u.Query()[k] {
				for _, hp := range strings.Split(qv, ",") {
					hosts = append(hosts, hostOnly(hp))
				}
			}
		}
		return hosts, nil
	case strings.Contains(v, "="):
		var hosts []string
		for _, f := range strings.Fields(v) {
			k, val, ok := strings.Cut(f, "=")
			if ok && (k == "host" || k == "hostaddr") {
				for _, hp := range strings.Split(strings.Trim(val, "'"), ",") {
					hosts = append(hosts, hostOnly(hp))
				}
			}
		}
		return hosts, nil
	default:
		var hosts []string
		for _, hp := range strings.Split(v, ",") {
			hosts = append(hosts, hostOnly(strings.TrimSpace(hp)))
		}
		return hosts, nil
	}
}

// hostOnly strips a port from "host:port" or "[v6]:port".
func hostOnly(hp string) string {
	if h, _, err := net.SplitHostPort(hp); err == nil {
		return h
	}
	return strings.Trim(hp, "[]")
}

func isLocalHost(h string) bool {
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(h)
	return err == nil && ip.IsLoopback()
}

// approveAllWarning is printed to stderr before the run starts.
const approveAllWarning = `
################################################################
#  -approve-all IS ON                                          #
#  Every proposal the agent makes is approved by this process  #
#  and its steps are EXECUTED against the stack below.         #
#  This is allowed only for the local dev stack.               #
################################################################
`
