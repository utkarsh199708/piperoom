//go:build cassettegen

package aiven

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/setup/targets/managed/replay"
)

// TestGenerateCassettes writes testdata/aiven/<case>.json from the
// hand-written responses of avCases with the requests this client sends,
// marked Recorded false.
//
//	go test -tags cassettegen -run TestGenerateCassettes ./internal/clients/aiven/
func TestGenerateCassettes(t *testing.T) {
	for _, tc := range avCases {
		t.Run(tc.name, func(t *testing.T) {
			canned := replay.NewCanned(tc.responses...)
			rec := replay.NewRecorder(canned, Scrub, replay.ProviderAiven, tc.name, cassetteSource, time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			tc.run(t, context.Background(), rec)
			if canned.Left() != 0 {
				t.Fatalf("%d canned responses unused", canned.Left())
			}
			cs := rec.Cassette()
			cs.Recorded = false
			if err := replay.Save(cs, filepath.Join("testdata", "aiven", tc.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
