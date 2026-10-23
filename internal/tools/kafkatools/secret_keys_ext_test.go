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
