package room_test

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/alertin"
	"piperoom/internal/store/memstore"
)

// Alert bodies with a number that is not finite as a float64 are refused with
// 400 on both entry points, before anything is written. Big but finite numbers
// are accepted.
func TestAlertBodyGuardOutOfRangeNumbers(t *testing.T) {
	bad := map[string]string{
		"positive":        `{"title":"x","v":1e400}`,
		"negative":        `{"title":"x","v":-1e400}`,
		"capital E":       `{"v":2E999}`,
		"nested in array": `{"labels":[{"v":[1,3e500]}]}`,
		"huge exponent":   `{"v":1e99999999999}`,
	}
	for name, body := range bad {
		t.Run(name+"/webhook", func(t *testing.T) {
			svc, runs, hooks := guardSetup(t, memstore.New())
			_, created, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", json.RawMessage(body))
			wantValidation400(t, err)
			assertNothingCreated(t, svc, runs, hooks, created)
		})
		t.Run(name+"/signed", func(t *testing.T) {
			svc, runs, hooks := guardSetup(t, memstore.New())
			_, created, err := svc.OpenAlertRoom(context.Background(), alertin.Alert{
				Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2",
				PipelineID: "orders", DedupKey: "generic:k", Raw: json.RawMessage(body),
			})
			wantValidation400(t, err)
			assertNothingCreated(t, svc, runs, hooks, created)
		})
	}

	good := map[string]string{
		"largest double":      `{"v":1.7976931348623157e308}`,
		"above 2^53":          `{"v":9007199254740993}`,
		"twenty digits":       `{"v":12345678901234567890}`,
		"underflow":           `{"v":1e-400}`,
		"text that looks big": `{"v":"1e400"}`,
	}
	for name, body := range good {
		t.Run(name, func(t *testing.T) {
			svc, _, _ := guardSetup(t, memstore.New())
			_, created, err := svc.CreateRoomFromAlert(context.Background(), "lag", "sev2", "orders", json.RawMessage(body))
			if err != nil || !created {
				t.Fatalf("created=%v err=%v, want the room opened", created, err)
			}
		})
	}
}
