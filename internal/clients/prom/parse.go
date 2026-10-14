// Package prom reads a Prometheus text-format endpoint (the JMX exporter next
// to Kafka Connect) with the standard library only.
package prom

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Sample is one metric line: name, labels and value. The timestamp, if the
// line has one, is dropped.
type Sample struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
}

// maxLineBytes bounds one exposition line.
const maxLineBytes = 64 << 10

// Parse reads the text exposition format: "name{label=\"v\",...} value
// [timestamp]" lines, "#" comment lines (HELP, TYPE) and blank lines. Label
// values may escape \\, \" and \n. A malformed line is an error naming its
// number, so an endpoint that is not Prometheus (an HTML page) fails loudly.
func Parse(r io.Reader) ([]Sample, error) {
	var out []Sample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 4096), maxLineBytes)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		s, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("prom: line %d: %w", n, err)
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("prom: read: %w", err)
	}
	return out, nil
}

func parseLine(line string) (Sample, error) {
	i := 0
	for i < len(line) && isNameChar(line[i], i == 0) {
		i++
	}
	if i == 0 {
		return Sample{}, fmt.Errorf("no metric name")
	}
	s := Sample{Name: line[:i]}
	rest := line[i:]
	if strings.HasPrefix(rest, "{") {
		labels, after, err := parseLabels(rest[1:])
		if err != nil {
			return Sample{}, err
		}
		s.Labels, rest = labels, after
	}
	fields := strings.Fields(rest)
	if len(fields) < 1 || len(fields) > 2 {
		return Sample{}, fmt.Errorf("want a value and an optional timestamp after the name")
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return Sample{}, fmt.Errorf("value %q is not a number", cut(fields[0]))
	}
	if len(fields) == 2 {
		if _, err := strconv.ParseInt(fields[1], 10, 64); err != nil {
			return Sample{}, fmt.Errorf("timestamp %q is not an integer", cut(fields[1]))
		}
	}
	s.Value = v
	return s, nil
}

// parseLabels reads `a="x",b="y"}` (the text after the opening brace) and
// returns the labels and the text after the closing brace.
func parseLabels(s string) (map[string]string, string, error) {
	labels := map[string]string{}
	for {
		s = strings.TrimLeft(s, " \t")
		if strings.HasPrefix(s, "}") {
			return labels, s[1:], nil
		}
		i := 0
		for i < len(s) && isNameChar(s[i], i == 0) && s[i] != ':' {
			i++
		}
		if i == 0 {
			return nil, "", fmt.Errorf("bad label name")
		}
		name := s[:i]
		s = strings.TrimLeft(s[i:], " \t")
		if !strings.HasPrefix(s, "=") {
			return nil, "", fmt.Errorf("label %q has no '='", cut(name))
		}
		s = strings.TrimLeft(s[1:], " \t")
		if !strings.HasPrefix(s, `"`) {
			return nil, "", fmt.Errorf("label %q value is not quoted", cut(name))
		}
		var val strings.Builder
		j := 1
		closed := false
		for j < len(s) {
			c := s[j]
			if c == '\\' {
				if j+1 >= len(s) {
					break
				}
				switch s[j+1] {
				case 'n':
					val.WriteByte('\n')
				case '\\', '"':
					val.WriteByte(s[j+1])
				default: // unknown escape: keep both bytes, as the reference parser does
					val.WriteByte('\\')
					val.WriteByte(s[j+1])
				}
				j += 2
				continue
			}
			if c == '"' {
				closed = true
				j++
				break
			}
			val.WriteByte(c)
			j++
		}
		if !closed {
			return nil, "", fmt.Errorf("label %q value is not terminated", cut(name))
		}
		labels[name] = val.String()
		s = strings.TrimLeft(s[j:], " \t")
		switch {
		case strings.HasPrefix(s, ","):
			s = s[1:]
		case strings.HasPrefix(s, "}"):
		default:
			return nil, "", fmt.Errorf("expected ',' or '}' after label %q", cut(name))
		}
	}
}

func isNameChar(c byte, first bool) bool {
	switch {
	case c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		return true
	case c >= '0' && c <= '9':
		return !first
	}
	return false
}

// cut shortens text echoed in an error: it comes from the remote endpoint.
func cut(s string) string {
	if len(s) > 40 {
		return s[:40] + "…"
	}
	return s
}
