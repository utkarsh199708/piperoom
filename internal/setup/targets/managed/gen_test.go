//go:build cassettegen

package managed

import (
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/setup/ops"
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

// TestGenerateConfluentFlows writes testdata/confluent/<flow>.json.
func TestGenerateConfluentFlows(t *testing.T) {
	for _, f := range ccFlows {
		t.Run(f.name, func(t *testing.T) {
			canned := replay.NewCanned(f.responses...)
			rec := replay.NewRecorder(canned, replay.Scrubber{}, replay.ProviderConfluent, f.name, ccSource, time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			var reg *ops.Registry
			if f.acl {
				reg = newConfluent(t, replay.NewCanned(), rec)
			} else {
				reg = newConfluent(t, rec, replay.NewCanned())
			}
			res, err := exec(t, reg, f.op, f.params)
			f.check(t, res, err)
			if canned.Left() != 0 {
				t.Fatalf("%d canned responses unused", canned.Left())
			}
			cs := rec.Cassette()
			cs.Recorded = false
			if err := replay.Save(cs, filepath.Join("testdata", "confluent", f.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestGenerateAivenFlows writes testdata/aiven/<flow>.json.
func TestGenerateAivenFlows(t *testing.T) {
	for _, f := range avFlows {
		t.Run(f.name, func(t *testing.T) {
			canned := replay.NewCanned(f.responses...)
			rec := replay.NewRecorder(canned, replay.Scrubber{}, replay.ProviderAiven, f.name, avSource, time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC))
			reg := newAiven(t, rec)
			res, err := exec(t, reg, f.op, f.params)
			f.check(t, res, err)
			if canned.Left() != 0 {
				t.Fatalf("%d canned responses unused", canned.Left())
			}
			cs := rec.Cassette()
			cs.Recorded = false
			if err := replay.Save(cs, filepath.Join("testdata", "aiven", f.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
