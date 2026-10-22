package actions_test

import (
	"testing"

	"piperoom/internal/actions"
)

// ConfigDriftSignal.ChangedKeys lists a key by name only when LooksSecret
// says no (X3-0 TC2): these are the shapes the watcher must hide.
func TestLooksSecretCoversConfigDriftKeys(t *testing.T) {
	for _, k := range []string{"database.password", "sasl.jaas.config", "ssl.key.password", "api.key", "connection.token"} {
		if !actions.LooksSecret(k) {
			t.Errorf("LooksSecret(%q) = false; config_drift would list it by name", k)
		}
	}
	for _, k := range []string{"snapshot.mode", "table.include.list", "tasks.max"} {
		if actions.LooksSecret(k) {
			t.Errorf("LooksSecret(%q) = true; config_drift would hide a plain key", k)
		}
	}
}
