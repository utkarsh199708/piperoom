package redact

import (
	"strings"
	"testing"
)

func TestCLIFlagAndXMLSecrets(t *testing.T) {
	const sec = "Zx9hunter2Qw7Pa55"
	leaks := []string{
		"curl -u admin:" + sec + " http://connect:8083/connectors",
		"curl --user admin:" + sec + " http://connect:8083/connectors",
		"psql --password " + sec,
		"kafka-console-consumer --sasl-password " + sec,
		"confluent login --api-secret " + sec + "aB3dE6fG9hJ2kL5mN8pQ1rS4tU7vW0xY3z",
		"snowsql --private-key-passphrase '" + sec + "'",
		"<password>" + sec + "</password>",
		"<ssl.keystore.password>" + sec + "</ssl.keystore.password>",
	}
	for _, in := range leaks {
		if got, _ := Text(in); strings.Contains(got, sec) {
			t.Errorf("leak: %q -> %q", in, got)
		}
	}
	// Evidence that only looks like these forms stays as it is.
	keep := []string{
		"pg_dump --password-file /etc/pg/pass orders",
		"kafka-topics --token-ttl 3600",
		"curl -u: http://connect:8083/",
		"run --secret-name orders-db",
		"<passwordPolicy>strict</passwordPolicy>",
		"connector pg-orders-connector task 0 RUNNING",
	}
	for _, in := range keep {
		if got, _ := Text(in); got != in {
			t.Errorf("changed evidence: %q -> %q", in, got)
		}
	}
}
