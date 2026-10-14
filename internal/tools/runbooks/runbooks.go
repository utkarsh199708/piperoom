// Package runbooks gives the investigation agent short, sourced runbook
// sections for common CDC/Kafka failure modes (tool runbook_lookup). The
// runbooks are reviewed markdown files embedded from library/*.md; the tool
// reads only that text and never touches a customer system.
package runbooks

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// maxFileBytes caps one runbook file so a section stays a short read.
const maxFileBytes = 8192

// Section is one "## " part of a runbook.
type Section struct{ Heading, Text string }

// Runbook is one parsed library file.
type Runbook struct {
	ID, Title string
	Keywords  []string
	Sections  []Section // Symptoms … Risks, in file order (Sources is not a section)
	Tools     []string  // from "Find it with Piperoom tools"
	Actions   []string  // from Fixes items that start with: Action `name`
	Sources   []string  // https URLs from "Sources"
}

// headings are the required "## " headings, in order.
var headings = []string{"Symptoms", "Find it with Piperoom tools", "Safe checks", "Fixes", "Risks", "Sources"}

// allowedHosts are the documentation sites a runbook may cite.
var allowedHosts = []string{"www.postgresql.org", "debezium.io", "kafka.apache.org", "docs.confluent.io"}

//go:embed library/*.md
var library embed.FS

// Embedded returns the runbooks shipped in library/*.md.
func Embedded() ([]Runbook, error) {
	sub, err := fs.Sub(library, "library")
	if err != nil {
		return nil, fmt.Errorf("runbooks: embedded library: %w", err)
	}
	return Load(sub)
}

// Load parses every *.md at the root of fsys, sorted by file name. A file that
// breaks the format is an error, so a bad runbook fails at start-up.
func Load(fsys fs.FS) ([]Runbook, error) {
	names, err := fs.Glob(fsys, "*.md")
	if err != nil {
		return nil, fmt.Errorf("runbooks: list library: %w", err)
	}
	sort.Strings(names)
	var out []Runbook
	seen := map[string]bool{}
	for _, name := range names {
		b, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("runbooks: %s: %w", name, err)
		}
		rb, err := parse(strings.TrimSuffix(path.Base(name), ".md"), b)
		if err != nil {
			return nil, fmt.Errorf("runbooks: %s: %w", name, err)
		}
		if seen[rb.ID] {
			return nil, fmt.Errorf("runbooks: %s: duplicate id %q", name, rb.ID)
		}
		seen[rb.ID] = true
		out = append(out, rb)
	}
	return out, nil
}

var (
	toolItemRe   = regexp.MustCompile("^`([a-z0-9_]+)`")
	actionItemRe = regexp.MustCompile("^Action ((?:`[a-z0-9_]+`(?: / )?)+)")
	backtickRe   = regexp.MustCompile("`([a-z0-9_]+)`")
)

func parse(fileID string, b []byte) (Runbook, error) {
	if len(b) > maxFileBytes {
		return Runbook{}, fmt.Errorf("%d bytes, more than %d", len(b), maxFileBytes)
	}
	text := strings.ReplaceAll(string(b), "\r\n", "\n")
	lines := strings.Split(text, "\n")

	var rb Runbook
	body, err := frontMatter(lines, &rb)
	if err != nil {
		return Runbook{}, err
	}
	switch {
	case rb.ID == "" || rb.Title == "" || len(rb.Keywords) == 0:
		return Runbook{}, errors.New("front matter needs id, title and keywords")
	case rb.ID != fileID:
		return Runbook{}, fmt.Errorf("id %q must equal the file name %q", rb.ID, fileID)
	}

	sections, err := splitSections(body)
	if err != nil {
		return Runbook{}, err
	}
	for _, s := range sections {
		items := listItems(s.Text)
		switch s.Heading {
		case "Find it with Piperoom tools":
			for _, it := range items {
				m := toolItemRe.FindStringSubmatch(it)
				if m == nil {
					return Runbook{}, fmt.Errorf("%q item %q must start with a backticked tool name", s.Heading, it)
				}
				rb.Tools = append(rb.Tools, m[1])
			}
		case "Fixes":
			for _, it := range items {
				if m := actionItemRe.FindStringSubmatch(it); m != nil {
					for _, a := range backtickRe.FindAllStringSubmatch(m[1], -1) {
						if !slices.Contains(rb.Actions, a[1]) {
							rb.Actions = append(rb.Actions, a[1])
						}
					}
					continue
				}
				if !strings.HasPrefix(it, "Manual") {
					return Runbook{}, fmt.Errorf("Fixes item %q must start with Action `name` or Manual", it)
				}
			}
		case "Sources":
			for _, it := range items {
				f := strings.Fields(it)
				if len(f) == 0 {
					return Runbook{}, errors.New("empty Sources item")
				}
				u := f[0]
				if err := checkSource(u); err != nil {
					return Runbook{}, err
				}
				rb.Sources = append(rb.Sources, u)
			}
			if len(rb.Sources) == 0 {
				return Runbook{}, errors.New("Sources is empty")
			}
			continue // Sources is not a lookup section
		}
		rb.Sections = append(rb.Sections, s)
	}
	return rb, nil
}

func frontMatter(lines []string, rb *Runbook) ([]string, error) {
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return nil, errors.New("missing front matter (first line must be ---)")
	}
	for i := 1; i < len(lines); i++ {
		l := strings.TrimSpace(lines[i])
		if l == "---" {
			return lines[i+1:], nil
		}
		key, val, ok := strings.Cut(l, ":")
		if !ok {
			return nil, fmt.Errorf("front matter line %q is not key: value", l)
		}
		val = strings.TrimSpace(val)
		switch strings.TrimSpace(key) {
		case "id":
			rb.ID = val
		case "title":
			rb.Title = val
		case "keywords":
			for _, k := range strings.Split(val, ",") {
				if k = strings.TrimSpace(k); k != "" {
					rb.Keywords = append(rb.Keywords, k)
				}
			}
		default:
			return nil, fmt.Errorf("unknown front matter key %q", key)
		}
	}
	return nil, errors.New("front matter is not closed with ---")
}

// splitSections enforces exactly the required headings, in order.
func splitSections(body []string) ([]Section, error) {
	var out []Section
	var cur *Section
	var text []string
	flush := func() {
		if cur != nil {
			cur.Text = strings.TrimSpace(strings.Join(text, "\n"))
			out = append(out, *cur)
		}
	}
	for _, l := range body {
		if h, ok := strings.CutPrefix(l, "## "); ok {
			flush()
			h = strings.TrimSpace(h)
			if i := len(out); i >= len(headings) || headings[i] != h {
				return nil, fmt.Errorf("section %q is extra or out of order (want %s)", h, strings.Join(headings, ", "))
			}
			cur, text = &Section{Heading: h}, nil
			continue
		}
		if cur == nil {
			if strings.TrimSpace(l) != "" {
				return nil, fmt.Errorf("text before the first section: %q", l)
			}
			continue
		}
		text = append(text, l)
	}
	flush()
	if len(out) != len(headings) {
		return nil, fmt.Errorf("missing sections: want %s", strings.Join(headings, ", "))
	}
	return out, nil
}

// listItems returns the "- " items of a section, without the marker.
func listItems(text string) []string {
	items := []string{} // non-nil: hit data says "items":[], not null
	for _, l := range strings.Split(text, "\n") {
		if it, ok := strings.CutPrefix(strings.TrimSpace(l), "- "); ok {
			items = append(items, strings.TrimSpace(it))
		}
	}
	return items
}

func checkSource(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || !slices.Contains(allowedHosts, u.Host) {
		return fmt.Errorf("source %q must be https on one of %s", raw, strings.Join(allowedHosts, ", "))
	}
	return nil
}
