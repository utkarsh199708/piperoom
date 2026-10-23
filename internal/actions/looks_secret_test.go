package actions

import "testing"

// LooksSecret is the shared rule (U0): it errs towards true, and the
// connector-config action now uses it (U1-23) instead of a private copy.
func TestLooksSecretRule(t *testing.T) {
	secret := []string{
		"database.password", "DATABASE.PASSWORD", "db.passwd", "sasl.jaas.config", "client.secret",
		"auth.token", "aws.credential.file", "confluent.apikey", "ssl.key.password", "api.key",
		"key.converter.schema.registry.basic.auth.user.info", "private_key_path", "schema.registry.basic.auth.credentials.source",
		// Credentials of the U3a sink index the old word list missed.
		"debezium.sink.eventhubs.connectionstring", "debezium.sink.nats-jetstream.auth.seed",
		"debezium.sink.nats-jetstream.auth.jwt", "http.headers.authorization", "HTTP.HEADERS.AUTHORIZATION",
	}
	plain := []string{
		"database.hostname", "database.port", "topic.prefix", "table.include.list", "plugin.name", "slot.name",
		"publication.name", "snapshot.mode", "tasks.max", "connector.class", "keyspace", "keyed.topic", "monkey",
		"", "heartbeat.interval.ms",
		// The last-segment words do not flag a key that only mentions them,
		// and a URL key is checked by its value, not its name.
		"http.authorization.type", "debezium.sink.http.authentication.jwt.url", "random.seed.bytes",
		"connection.url", "mongodb.connection.string",
	}
	for _, k := range secret {
		if !LooksSecret(k) {
			t.Errorf("LooksSecret(%q) = false", k)
		}
	}
	for _, k := range plain {
		if LooksSecret(k) {
			t.Errorf("LooksSecret(%q) = true", k)
		}
	}
}
