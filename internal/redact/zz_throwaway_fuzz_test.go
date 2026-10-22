package redact

import "testing"

func FuzzThrowawayOK(f *testing.F) {
	f.Add("a")
	f.Fuzz(func(t *testing.T, s string) {})
}

func FuzzThrowawayFail(f *testing.F) {
	f.Add("a")
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 3 {
			t.Fatal("throwaway crasher")
		}
	})
}
