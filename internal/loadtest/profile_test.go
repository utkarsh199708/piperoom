package loadtest

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

var testSpecs = []ParamSpec{
	{Name: "rooms", Kind: KindInt, Full: "50", Short: "5", Min: 1},
	{Name: "agents", Kind: KindIntList, Full: "3,10,30", Short: "2", Min: 1},
	{Name: "latency", Kind: KindDuration, Full: "300ms", Min: 0},
	{Name: "rate", Kind: KindFloat, Full: "1.5", Min: 0.1},
	{Name: "burst", Kind: KindBool, Full: "false"},
}

func TestParseOverrides(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    []Override
		wantErr bool
	}{
		{"none", nil, []Override{}, false},
		{"plain", []string{"rooms=10"}, []Override{{Key: "rooms", Value: "10"}}, false},
		{"spaces trimmed", []string{" rooms = 10 "}, []Override{{Key: "rooms", Value: "10"}}, false},
		{"scoped", []string{"fairness.rooms=10"}, []Override{{Profile: "fairness", Key: "rooms", Value: "10"}}, false},
		{"list value keeps commas", []string{"agents=3,10"}, []Override{{Key: "agents", Value: "3,10"}}, false},
		{"value may hold =", []string{"x=a=b"}, []Override{{Key: "x", Value: "a=b"}}, false},
		{"missing =", []string{"rooms"}, nil, true},
		{"empty key", []string{"=3"}, nil, true},
		{"empty value", []string{"rooms="}, nil, true},
		{"empty profile", []string{".rooms=3"}, nil, true},
		{"empty scoped key", []string{"fairness.=3"}, nil, true},
		{"duplicate", []string{"rooms=1", "rooms=2"}, nil, true},
		{"same key different scope is fine", []string{"rooms=1", "fairness.rooms=2"},
			[]Override{{Key: "rooms", Value: "1"}, {Profile: "fairness", Key: "rooms", Value: "2"}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseOverrides(tc.args)
			if tc.wantErr {
				if !errors.Is(err, ErrBadParam) {
					t.Fatalf("err = %v, want ErrBadParam", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseParamsDefaultsAndOverrides(t *testing.T) {
	full, err := ParseParams("p", testSpecs, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if full.Int("rooms") != 50 || !reflect.DeepEqual(full.Ints("agents"), []int{3, 10, 30}) ||
		full.Dur("latency") != 300*time.Millisecond || full.Float("rate") != 1.5 || full.Bool("burst") {
		t.Fatalf("full defaults wrong: %+v", full.values)
	}
	short, err := ParseParams("p", testSpecs, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Short falls back to Full when no short default is declared.
	if short.Int("rooms") != 5 || !reflect.DeepEqual(short.Ints("agents"), []int{2}) || short.Dur("latency") != 300*time.Millisecond {
		t.Fatalf("short defaults wrong: %+v", short.values)
	}
	ov := []Override{
		{Key: "rooms", Value: "7"},
		{Profile: "p", Key: "latency", Value: "1s"},
		{Profile: "other", Key: "rooms", Value: "99"}, // another profile's: ignored
		{Key: "unknown_here", Value: "1"},             // unscoped and unknown: ignored here
		{Key: "burst", Value: "true"},
	}
	got, err := ParseParams("p", testSpecs, false, ov)
	if err != nil {
		t.Fatal(err)
	}
	if got.Int("rooms") != 7 || got.Dur("latency") != time.Second || !got.Bool("burst") {
		t.Fatalf("overrides not applied: %+v", got.values)
	}
	if got.Raw()["rooms"] != "7" {
		t.Fatalf("Raw = %v", got.Raw())
	}
	// Ints returns a copy.
	l := got.Ints("agents")
	l[0] = 1000
	if got.Ints("agents")[0] != 3 {
		t.Fatal("Ints must return a copy")
	}
	// Wrong-name getters return zero values rather than panicking.
	if got.Int("nope") != 0 || got.Dur("rooms") != 0 || got.Ints("rooms") != nil {
		t.Fatal("getters on unknown or mistyped names must return zero")
	}
}

func TestParseParamsErrors(t *testing.T) {
	tests := []struct {
		name string
		ov   Override
	}{
		{"not an int", Override{Key: "rooms", Value: "ten"}},
		{"below min", Override{Key: "rooms", Value: "0"}},
		{"list item not int", Override{Key: "agents", Value: "3,x"}},
		{"list empty item", Override{Key: "agents", Value: "3,,4"}},
		{"list below min", Override{Key: "agents", Value: "3,0"}},
		{"bad duration", Override{Key: "latency", Value: "300"}},
		{"negative duration", Override{Key: "latency", Value: "-1s"}},
		{"bad float", Override{Key: "rate", Value: "fast"}},
		{"NaN float", Override{Key: "rate", Value: "NaN"}},
		{"float below min", Override{Key: "rate", Value: "0.01"}},
		{"bad bool", Override{Key: "burst", Value: "maybe"}},
		{"scoped unknown key", Override{Profile: "p", Key: "nope", Value: "1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseParams("p", testSpecs, false, []Override{tc.ov})
			if !errors.Is(err, ErrBadParam) {
				t.Fatalf("err = %v, want ErrBadParam", err)
			}
		})
	}
}

func TestCheckOverrides(t *testing.T) {
	profiles := []Profile{{Name: "a", Params: testSpecs}, {Name: "b", Params: []ParamSpec{{Name: "only_b", Kind: KindInt, Full: "1"}}}}
	ok := [][]Override{
		{{Key: "rooms", Value: "1"}},
		{{Key: "only_b", Value: "1"}},
		{{Profile: "b", Key: "only_b", Value: "1"}},
	}
	for _, ov := range ok {
		if err := CheckOverrides(profiles, ov); err != nil {
			t.Fatalf("CheckOverrides(%+v) = %v", ov, err)
		}
	}
	bad := [][]Override{
		{{Key: "typo", Value: "1"}},
		{{Profile: "a", Key: "only_b", Value: "1"}},
		{{Profile: "c", Key: "rooms", Value: "1"}},
	}
	for _, ov := range bad {
		if err := CheckOverrides(profiles, ov); !errors.Is(err, ErrBadParam) {
			t.Fatalf("CheckOverrides(%+v) = %v, want ErrBadParam", ov, err)
		}
	}
}

// Every built-in profile's defaults must parse, full and short, so a typo in a
// default fails here and not halfway through a long run.
func TestBuiltinProfilesParse(t *testing.T) {
	ps := Profiles()
	if len(ps) == 0 {
		t.Fatal("no profiles")
	}
	seen := map[string]bool{}
	for _, pr := range ps {
		if seen[pr.Name] {
			t.Fatalf("duplicate profile %q", pr.Name)
		}
		seen[pr.Name] = true
		if pr.Run == nil {
			t.Fatalf("profile %q has no Run", pr.Name)
		}
		for _, short := range []bool{false, true} {
			if _, err := ParseParams(pr.Name, pr.Params, short, nil); err != nil {
				t.Fatalf("profile %q short=%v: %v", pr.Name, short, err)
			}
		}
	}
	for _, want := range []string{"fairness", "throughput", "replay", "fanout", "isolation"} {
		if !seen[want] {
			t.Fatalf("missing profile %q", want)
		}
	}
	if _, err := SelectProfiles("all"); err != nil {
		t.Fatal(err)
	}
	if _, err := SelectProfiles("fairness,nope"); err == nil {
		t.Fatal("unknown profile must be an error")
	}
	if got, err := SelectProfiles("fanout, fairness"); err != nil || len(got) != 2 || got[0].Name != "fanout" {
		t.Fatalf("SelectProfiles keeps the order given: %v %v", got, err)
	}
}
