package httpapi_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// docs/api.md is part of the API contract, but its prose repeats facts the
// code owns: how many layers the solo user gets from GET /api/me and how many
// incident layers a label accepts. These claims drifted once (KAN-396: the doc
// said eight while events.Layers had nine after the environment layer landed),
// so the test pins them to events.Layers and events.IncidentLayers().

// soloLayersClaim is "every entry of `events.Layers` (nine: postgres, ...)".
var soloLayersClaim = regexp.MustCompile("every entry of `events\\.Layers` \\(([a-z0-9]+): ([^)]*)\\)")

// incidentLayersClaim covers "the eight incident layers" and
// "`events.IncidentLayers()`, eight;" -- both spell out the count in words.
var incidentLayersClaim = regexp.MustCompile("the ([a-z0-9]+) incident layers|`events\\.IncidentLayers\\(\\)`, ([a-z0-9]+);")

func TestAPIDocLayerCounts(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "api.md"))
	if err != nil {
		t.Fatalf("read docs/api.md: %v", err)
	}
	text := string(doc)

	t.Run("solo GET /api/me lists every events.Layers entry", func(t *testing.T) {
		m := soloLayersClaim.FindStringSubmatch(text)
		if m == nil {
			t.Fatalf("docs/api.md no longer spells out the solo layers as %q; update this test with the new wording", soloLayersClaim)
		}
		if got, want := number(t, m[1]), len(events.Layers); got != want {
			t.Errorf("docs/api.md says the solo user gets %s layers, events.Layers has %d", m[1], want)
		}
		names := strings.Split(m[2], ", ")
		if !slices.Equal(names, events.Layers) {
			t.Errorf("docs/api.md lists %v, events.Layers is %v (names and order must match)", names, events.Layers)
		}
	})

	t.Run("incident layer counts match events.IncidentLayers", func(t *testing.T) {
		ms := incidentLayersClaim.FindAllStringSubmatch(text, -1)
		if len(ms) == 0 {
			t.Fatalf("docs/api.md no longer states an incident layer count as %q; update this test with the new wording", incidentLayersClaim)
		}
		for _, m := range ms {
			word := m[1]
			if word == "" {
				word = m[2]
			}
			if got, want := number(t, word), len(events.IncidentLayers()); got != want {
				t.Errorf("docs/api.md says %q incident layers, events.IncidentLayers() has %d", word, want)
			}
		}
	})
}

// number reads a count the doc may spell as a word ("nine") or a digit.
func number(t *testing.T, s string) int {
	t.Helper()
	words := map[string]int{
		"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
		"six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
		"eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
		"sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
	}
	if n, ok := words[s]; ok {
		return n
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("unrecognised count %q in docs/api.md", s)
	}
	return n
}
