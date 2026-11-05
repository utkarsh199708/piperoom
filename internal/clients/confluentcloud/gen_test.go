//go:build cassettegen

package confluentcloud

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/setup/targets/managed/replay"
)

// TestGenerateCassettes writes testdata/confluent/<case>.json from the
// hand-written responses of ccCases with the requests this client sends,
// marked Recorded false.
//
//	go test -tags cassettegen -run TestGenerateCassettes ./internal/clients/confluentcloud/
func TestGenerateCassettes(t *testing.T) {
	for _, tc := range ccCases {
		t.Run(tc.name, func(t *testing.T) {
			canned := replay.NewCanned(tc.responses...)
			rec := replay.NewRecorder(canned, Scrub, replay.ProviderConfluent, tc.name, cassetteSource, time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			tc.run(t, context.Background(), rec)
			if canned.Left() != 0 {
				t.Fatalf("%d canned responses unused", canned.Left())
			}
			cs := rec.Cassette()
			cs.Recorded = false
			if err := replay.Save(cs, filepath.Join("testdata", "confluent", tc.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
