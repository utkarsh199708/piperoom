package store_test

import (
	"errors"
	"testing"

	"piperoom/internal/store"
)

// esc builds a JSON \uXXXX escape without writing it as one in source text.
func esc(hex string) string { return "\\u" + hex }

func TestCheckText(t *testing.T) {
	for name, s := range map[string]string{
		"nul":                "a\x00b",
		"invalid utf-8":      "a\xffb",
		"truncated sequence": "a\xe4\xb8",
		"encoded surrogate":  "a\xed\xa0\x80",
	} {
		if err := store.CheckText("ok", s); !errors.Is(err, store.ErrUnstorable) {
			t.Errorf("%s: err = %v, want ErrUnstorable", name, err)
		}
	}
	if err := store.CheckText("", "plain", "café 中 \U0001F600", "�", `\u0000`); err != nil {
		t.Errorf("storable text refused: %v", err)
	}
	if err := store.CheckText(); err != nil {
		t.Errorf("no fields: %v", err)
	}
}

func TestCheckJSONAndSanitizeJSON(t *testing.T) {
	bad := map[string]string{
		"nul escape":                  `{"a":"x` + esc("0000") + `y"}`,
		"nul escape in key":           `{"a` + esc("0000") + `":1}`,
		"raw nul byte":                "{\"a\":\"x\x00y\"}",
		"invalid utf-8":               "{\"a\":\"x\xffy\"}",
		"lone high surrogate":         `{"a":"` + esc("d83d") + `"}`,
		"lone low surrogate":          `{"a":"` + esc("DE00") + `"}`,
		"high then plain char":        `{"a":"` + esc("d83d") + `A"}`,
		"high then high":              `{"a":"` + esc("d83d") + esc("d83d") + `"}`,
		"low then high":               `{"a":"` + esc("de00") + esc("d83d") + `"}`,
		"high at end":                 `"` + esc("D83D") + `"`,
		"nul after escaped backslash": `{"a":"\\` + esc("0000") + `"}`,
		"nested":                      `{"l":{"k":["a","b` + esc("0000") + `"]}}`,
	}
	for name, raw := range bad {
		if err := store.CheckJSON([]byte(raw)); !errors.Is(err, store.ErrUnstorable) {
			t.Errorf("%s: CheckJSON err = %v, want ErrUnstorable", name, err)
			continue
		}
		fixed := store.SanitizeJSON([]byte(raw))
		if err := store.CheckJSON(fixed); err != nil {
			t.Errorf("%s: sanitized JSON still refused: %v (%q)", name, err, fixed)
		}
	}

	good := map[string]string{
		"valid pair":               `{"a":"` + esc("d83d") + esc("de00") + `"}`,
		"escaped backslash u0000":  `{"a":"\\u0000"}`,
		"escaped bmp":              `{"a":"` + esc("00e9") + esc("4e2d") + `"}`,
		"quote then escape":        `{"a":"\"` + esc("0041") + `"}`,
		"literal utf-8":            "{\"a\":\"café 中 \U0001F600\"}",
		"literal replacement char": "{\"a\":\"�\"}",
		"empty":                    ``,
		"number":                   `1e5`,
	}
	for name, raw := range good {
		if err := store.CheckJSON([]byte(raw)); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
		if got := store.SanitizeJSON([]byte(raw)); string(got) != raw {
			t.Errorf("%s: SanitizeJSON changed %q to %q", name, raw, got)
		}
	}
	if store.CheckJSON(nil) != nil || store.SanitizeJSON(nil) != nil {
		t.Error("nil JSON must pass through")
	}

	// Exact output: each finding becomes U+FFFD and the text around it stays.
	cases := map[string]struct{ in, want string }{
		"nul escape":      {`"a` + esc("0000") + `b"`, `"a` + esc("fffd") + `b"`},
		"lone surrogate":  {`"a` + esc("d83d") + `b"`, `"a` + esc("fffd") + `b"`},
		"invalid byte":    {"\"a\xffb\"", "\"a�b\""},
		"raw nul":         {"\"a\x00b\"", "\"a�b\""},
		"two in one":      {`"` + esc("0000") + esc("de00") + `"`, `"` + esc("fffd") + esc("fffd") + `"`},
		"pair kept":       {`"` + esc("d83d") + esc("de00") + esc("0000") + `"`, `"` + esc("d83d") + esc("de00") + esc("fffd") + `"`},
		"escaped kept":    {`"\\u0000` + esc("0000") + `"`, `"\\u0000` + esc("fffd") + `"`},
		"valid text kept": {"\"é\xff中\"", "\"é�中\""},
	}
	for name, c := range cases {
		if got := string(store.SanitizeJSON([]byte(c.in))); got != c.want {
			t.Errorf("%s: SanitizeJSON(%q) = %q, want %q", name, c.in, got, c.want)
		}
	}
}
