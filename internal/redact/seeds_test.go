package redact

import "testing"

// TestManySeeds re-runs the fixture set with many fresh secret draws, so the
// zero-miss result does not hinge on one lucky seed.
func TestManySeeds(t *testing.T) {
	miss, total := 0, 0
	for seed := uint64(100); seed < 300; seed++ {
		for _, f := range buildFixtures(seed, 1) {
			out, _ := Text(f.text)
			for _, s := range f.secrets {
				total++
				if leaked(out, s) {
					miss++
					if miss < 15 {
						t.Errorf("seed %d missed %q\n  in:  %s\n  out: %s", seed, s, f.text, out)
					}
				}
			}
			if o2, _ := Text(out); o2 != out {
				t.Errorf("not idempotent:\n  once:  %s\n  twice: %s", out, o2)
			}
		}
	}
	t.Logf("secrets=%d missed=%d", total, miss)
}
