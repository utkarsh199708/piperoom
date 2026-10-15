package changes

import (
	"errors"
	"testing"
	"time"

	"piperoom/internal/tools"
)

// window is the only number-like input the model sends. Every spelling is
// either inside [MinWindow, MaxWindow] or refused as bad input; nothing is
// clamped, because a window longer than 7d would ask for a different answer
// than the model believes it got.
func TestWindowBounds(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		want   time.Duration // how far back since must be; unused when refuse
		refuse bool
	}{
		{"missing", `{}`, DefaultWindow, false},
		{"null", `{"window":null}`, DefaultWindow, false},
		{"empty", `{"window":""}`, DefaultWindow, false},
		{"in range", `{"window":"90m"}`, 90 * time.Minute, false},
		{"days", `{"window":"2d"}`, 48 * time.Hour, false},
		{"at min", `{"window":"1s"}`, MinWindow, false},
		{"at max in days", `{"window":"7d"}`, MaxWindow, false},
		{"at max in hours", `{"window":"168h"}`, MaxWindow, false},
		{"above max in days", `{"window":"8d"}`, 0, true},
		{"above max in hours", `{"window":"169h"}`, 0, true},
		{"huge days", `{"window":"1000000d"}`, 0, true},
		{"days beyond int64", `{"window":"99999999999999999999d"}`, 0, true},
		{"hours beyond int64", `{"window":"99999999999h"}`, 0, true},
		{"below min", `{"window":"999ms"}`, 0, true},
		{"one nanosecond", `{"window":"1ns"}`, 0, true},
		{"zero", `{"window":"0s"}`, 0, true},
		{"zero days", `{"window":"0d"}`, 0, true},
		{"negative", `{"window":"-1h"}`, 0, true},
		{"negative days", `{"window":"-1d"}`, 0, true},
		// -79311017351761 days times 24h wraps int64 to 65.536 microseconds, which
		// a check made after the multiplication takes for a valid window.
		{"negative days that wrap to a tiny window", `{"window":"-79311017351761d"}`, 0, true},
		{"most negative days", `{"window":"-9223372036854775808d"}`, 0, true},
		{"signed days", `{"window":"+3d"}`, 0, true},
		{"fractional days", `{"window":"1.5d"}`, 0, true},
		{"a number, not text", `{"window":6}`, 0, true},
		{"an exponent, not text", `{"window":1e30}`, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := &fakeLog{}
			_, err := run(t, newTool(l, nil), scoped(), tc.input)
			if tc.refuse {
				if !errors.Is(err, tools.ErrBadInput) {
					t.Fatalf("err = %v, want ErrBadInput", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := t0.Sub(l.got.since); got != tc.want {
				t.Errorf("since is %v ago, want %v", got, tc.want)
			}
		})
	}
}
