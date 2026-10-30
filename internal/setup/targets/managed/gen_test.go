//go:build cassettegen

package managed

import (
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/setup/targets/managed/replay"
)

// TestGenerateMSKFlows writes testdata/msk/<flow>.json from the
// hand-written responses of mskFlows, with the requests the ops really send
// (a Recorder over replay.Canned), marked Recorded false.
//
//	go test -tags cassettegen -run TestGenerate ./internal/setup/targets/managed/
func TestGenerateMSKFlows(t *testing.T) {
	for _, f := range mskFlows {
		t.Run(f.name, func(t *testing.T) {
			canned := replay.NewCanned(f.responses...)
			rec := replay.NewRecorder(canned, replay.Scrubber{}, replay.ProviderAWS, f.name, mskSource, time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			c, err := mskconnect.New(awsConfig(rec))
			if err != nil {
				t.Fatal(err)
			}
			reg, _ := newMSK(t, c, "", nil)
			res, err := exec(t, reg, f.op, f.params)
			f.check(t, res, err)
			if canned.Left() != 0 {
				t.Fatalf("%d canned responses unused", canned.Left())
			}
			cs := rec.Cassette()
			cs.Recorded = false
			if err := replay.Save(cs, filepath.Join("testdata", "msk", f.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
