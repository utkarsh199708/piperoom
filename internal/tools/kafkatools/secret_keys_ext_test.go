package kafkatools_test

import (
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/tools/kafkatools"
)

// The four allow-listed keys are not credentials by either rule: the tool's
// own sensitiveKey and the shared actions.LooksSecret.
func TestSignalAndPublicationKeysNotSecret(t *testing.T) {
	for _, k := range []string{"signal.data.collection", "signal.enabled.channels", "publication.name", "publication.autocreate.mode"} {
		if kafkatools.SensitiveKey(k) || actions.LooksSecret(k) {
			t.Errorf("%q reads as secret", k)
		}
	}
	for _, k := range []string{"signal.kafka.consumer.sasl.password", "publication.token"} {
		if !kafkatools.SensitiveKey(k) || !actions.LooksSecret(k) {
			t.Errorf("%q does not read as secret", k)
		}
	}
}

// The validator's private looksSecretKey is a copy of actions.LooksSecret
// (actions imports kafkatools, so the validator cannot import it back). The
// two must give the same answer for every key until a leaf package owns the
// rule; a key the shared rule flags but the copy misses would let a plaintext
// credential through connect_validate_config.
func TestLooksSecretKeyMatchesSharedRule(t *testing.T) {
	keys := []string{
		"database.password", "db.PASSWD", "sasl.jaas.config", "api.token",
		"aws.credentials.provider", "apikey", "ssl.truststore.private",
		"basic.auth.user.info.secret", "api.key", "ssl.key.password",
		"key.converter", "message.key.columns", "key.ignore", "record_key",
		"monkey", "keystone.url", "database.hostname", "tasks.max",
		"topic.prefix", "value.converter.schema.registry.url", "snapshot.mode",
		"transforms.route.regex", "KEY", "a.key.b", "privateLink.id", "",
	}
	for _, k := range keys {
		if got, want := kafkatools.LooksSecretKey(k), actions.LooksSecret(k); got != want {
			t.Errorf("looksSecretKey(%q) = %v, actions.LooksSecret = %v; the copies drifted", k, got, want)
		}
	}
}
