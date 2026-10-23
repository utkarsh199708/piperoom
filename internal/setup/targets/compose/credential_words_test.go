package compose

import (
	"testing"

	"piperoom/internal/actions"
)

// looksCredential is the artifact validator's copy of actions.LooksSecret's
// credential words (without the bare "key" segment rule). The two must agree
// on every credential key, in Java property and in environment spelling.
func TestLooksCredentialMatchesSharedWords(t *testing.T) {
	secret := []string{
		"database.password", "sasl.jaas.config", "api.token", "client.secret",
		"debezium.sink.eventhubs.connectionstring", "DEBEZIUM_SINK_EVENTHUBS_CONNECTIONSTRING",
		"debezium.sink.nats-jetstream.auth.seed", "DEBEZIUM_SINK_NATS_JETSTREAM_AUTH_SEED",
		"debezium.sink.nats-jetstream.auth.jwt", "DEBEZIUM_SINK_NATS_JETSTREAM_AUTH_JWT",
		"http.headers.authorization", "HTTP_HEADERS_AUTHORIZATION",
	}
	for _, k := range secret {
		if !looksCredential(k) {
			t.Errorf("looksCredential(%q) = false", k)
		}
		if !actions.LooksSecret(k) {
			t.Errorf("actions.LooksSecret(%q) = false; the rules drifted", k)
		}
	}
	plain := []string{
		"http.authorization.type", "HTTP_AUTHORIZATION_TYPE", "debezium.sink.http.authentication.jwt.url",
		"connection.url", "database.hostname",
	}
	for _, k := range plain {
		if looksCredential(k) {
			t.Errorf("looksCredential(%q) = true", k)
		}
		if actions.LooksSecret(k) {
			t.Errorf("actions.LooksSecret(%q) = true", k)
		}
	}
	// Separators are dropped here, so a connection string in any spelling
	// reads as connectionstring: stricter than the connector-config rule,
	// which leaves mongodb.connection.string to its host guard and value
	// check. A stack file never needs a plain DSN with credentials.
	for _, k := range []string{"mongodb.connection.string", "MONGODB_CONNECTION_STRING"} {
		if !looksCredential(k) {
			t.Errorf("looksCredential(%q) = false", k)
		}
	}
}
