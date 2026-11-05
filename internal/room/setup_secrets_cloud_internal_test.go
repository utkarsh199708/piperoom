package room

import (
	"encoding/json"
	"testing"

	"piperoom/internal/events"
)

// TestSetupSecretProjectionDropsCloudNames: a setup.secret_set naming a
// cloud setup credential (a log written before the route refused it) never
// becomes a name RegisterSetupCredentials asks the runner for.
func TestSetupSecretProjectionDropsCloudNames(t *testing.T) {
	snap := RoomSnapshot{Setup: &SetupSnapshot{}}
	for i, name := range []string{"source_password", "ccloud_api_secret", "AWS_SECRET_ACCESS_KEY", "aiven_token", "read_key"} {
		raw, err := json.Marshal(events.SetupSecretSetPayload{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		snap = applySetup(snap, events.Event{Seq: int64(i + 1), Type: events.SetupSecretSet, Payload: raw})
	}
	got := snap.Setup.secretNames
	if len(got) != 2 || got[0] != "source_password" || got[1] != "read_key" {
		t.Fatalf("secretNames = %v, want [source_password read_key]", got)
	}
}
