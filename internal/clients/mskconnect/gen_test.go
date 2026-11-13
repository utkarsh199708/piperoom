//go:build cassettegen

package mskconnect

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/setup/targets/managed/replay"
)

// TestGenerateCassettes writes testdata/aws/<case>.json from the
// hand-written responses of mskCases, with the requests the SDK really sends
// (a Recorder over replay.Canned). The cassettes are marked Recorded false:
// the responses follow the SDK's documented shapes and are not from a live
// account. The manual test (manual_test.go) records real ones.
//
//	go test -tags cassettegen -run TestGenerateCassettes ./internal/clients/mskconnect/
func TestGenerateCassettes(t *testing.T) {
	for _, tc := range mskCases {
		t.Run(tc.name, func(t *testing.T) {
			canned := replay.NewCanned(tc.responses...)
			rec := replay.NewRecorder(canned, Scrub, replay.ProviderAWS, tc.name, cassetteSource, time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			c, err := New(testConfig(rec))
			if err != nil {
				t.Fatal(err)
			}
			tc.run(t, context.Background(), c)
			if canned.Left() != 0 {
				t.Fatalf("%d canned responses unused", canned.Left())
			}
			cs := rec.Cassette()
			cs.Recorded = false
			if err := replay.Save(cs, filepath.Join("testdata", "aws", tc.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
