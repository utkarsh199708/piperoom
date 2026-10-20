package actions

import "testing"

// LooksSecret is the shared rule (U0): it errs towards true, and it agrees
// with the connector-config action's private looksSecret until A2 switches
// that copy to it (U1-23).
func TestLooksSecretRule(t *testing.T) {
	secret := []string{
		"database.password", "DATABASE.PASSWORD", "db.passwd", "sasl.jaas.config", "client.secret",
		"auth.token", "aws.credential.file", "confluent.apikey", "ssl.key.password", "api.key",
		"key.converter.schema.registry.basic.auth.user.info", "private_key_path", "schema.registry.basic.auth.credentials.source",
	}
	plain := []string{
		"database.hostname", "database.port", "topic.prefix", "table.include.list", "plugin.name", "slot.name",
		"publication.name", "snapshot.mode", "tasks.max", "connector.class", "keyspace", "keyed.topic", "monkey",
		"", "heartbeat.interval.ms",
	}
	for _, k := range secret {
		if !LooksSecret(k) {
			t.Errorf("LooksSecret(%q) = false", k)
		}
		if looksSecret(k) != LooksSecret(k) {
			t.Errorf("LooksSecret(%q) disagrees with the private copy", k)
		}
	}
	for _, k := range plain {
		if LooksSecret(k) {
			t.Errorf("LooksSecret(%q) = true", k)
		}
		if looksSecret(k) != LooksSecret(k) {
			t.Errorf("LooksSecret(%q) disagrees with the private copy", k)
		}
	}
}
