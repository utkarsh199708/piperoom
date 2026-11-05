//go:build xwavegen

package main

import (
	"math/rand/v2"
	"time"

	"piperoom/internal/owners"
)

func filteredIdleState() *xstate {
	rng := rand.New(rand.NewPCG(14, 2))
	s := baseState(rng, fx(8, 47, 13, 402))
	filtering := func(r orow, _ string) (orow, bool) { return r, r.status != "backfill" }
	for g := int64(1); g <= 40; g++ {
		s.insert(fx(9, 22, 5, 0), g, 900+g, "backfill", filtering)
	}
	s.insert(fx(9, 22, 5, 700), 77, 4200, "placed", filtering)
	return s
}

// droppedLoadState: load at 1/s from 07:30; the build without updated_at from 08:29:40.
func droppedLoadState() *xstate {
	rng := rand.New(rand.NewPCG(15, 2))
	s := baseState(rng, fx(7, 29, 58, 0))
	deploy := fx(8, 29, 40, 0)
	nullTS := func(r orow, _ string) (orow, bool) { r.updNull = true; return r, true }
	for t := fx(7, 30, 0, 0); t.Before(fx(9, 40, 0, 0)); t = t.Add(time.Second) {
		sink := goodSink
		if !t.Before(deploy) {
			sink = nullTS
		}
		s.insert(t, int64(rng.IntN(1000)), 5, "placed", sink)
	}
	return s
}

func probes(o *owners.Owners) {
	for _, s := range []scen{
		{file: "probe: sink-filtered-rows on an idle stack", build: filteredIdleState, watchFrom: fx(9, 22, 30, 0), watchUntil: fx(12, 40, 0, 0)},
		{file: "probe: sink-dropped-column under load", build: droppedLoadState, watchFrom: fx(8, 30, 0, 0), watchUntil: fx(9, 40, 0, 0)},
	} {
		runWatcher(s, o)
	}
}
