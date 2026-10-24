package fixpr

import (
	"fmt"
	"strings"

	"piperoom/internal/redact"
)

// A line diff of one file, rendered as a unified diff with three lines of
// context (git's format, so an approver reads it as they would on the PR).

const (
	diffContext = 3
	// maxEditDistance bounds the Myers search: past it the changed middle is
	// shown as one removal and one addition. That diff is still exact, only
	// not minimal, and costs at most maxEditDistance² ints of memory on a file
	// the model chose.
	maxEditDistance = 1000
	noNewline       = `\ No newline at end of file`
)

type opKind byte

const (
	opEq  opKind = ' '
	opDel opKind = '-'
	opIns opKind = '+'
)

type diffOp struct {
	kind opKind
	text string // the line with its terminator, if any
}

// splitLines splits s into lines that keep their "\n", so "x" and "x\n" are
// different lines, as in git.
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	lines := strings.SplitAfter(s, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// diffLines returns the edit script from a to b.
func diffLines(a, b []string) []diffOp {
	pre := 0
	for pre < len(a) && pre < len(b) && a[pre] == b[pre] {
		pre++
	}
	suf := 0
	for suf < len(a)-pre && suf < len(b)-pre && a[len(a)-1-suf] == b[len(b)-1-suf] {
		suf++
	}
	ops := make([]diffOp, 0, len(a)+len(b))
	for _, l := range a[:pre] {
		ops = append(ops, diffOp{opEq, l})
	}
	ops = append(ops, myers(a[pre:len(a)-suf], b[pre:len(b)-suf])...)
	for _, l := range a[len(a)-suf:] {
		ops = append(ops, diffOp{opEq, l})
	}
	return ops
}

// myers is the greedy O((N+M)D) shortest edit script, keeping each round's
// frontier (2d+1 entries) for the walk back.
func myers(a, b []string) []diffOp {
	n, m := len(a), len(b)
	if n == 0 || m == 0 {
		return replaceAll(a, b)
	}
	limit := min(n+m, maxEditDistance)
	off := limit + 1
	v := make([]int, 2*limit+3)
	trace := make([][]int, 0, 16)
	for d := 0; d <= limit; d++ {
		trace = append(trace, append([]int(nil), v[off-d:off+d+1]...))
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[off+k-1] < v[off+k+1]) {
				x = v[off+k+1]
			} else {
				x = v[off+k-1] + 1
			}
			y := x - k
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			v[off+k] = x
			if x >= n && y >= m {
				return backtrack(trace, a, b)
			}
		}
	}
	return replaceAll(a, b)
}

func backtrack(trace [][]int, a, b []string) []diffOp {
	x, y := len(a), len(b)
	var rev []diffOp
	for d := len(trace) - 1; d >= 0; d-- {
		at := func(k int) int { return trace[d][k+d] } // the frontier before round d
		k := x - y
		var pk int
		if k == -d || (k != d && at(k-1) < at(k+1)) {
			pk = k + 1
		} else {
			pk = k - 1
		}
		px := 0
		if d > 0 {
			px = at(pk)
		}
		py := px - pk
		if d == 0 {
			px, py = 0, 0
		}
		for x > px && y > py {
			rev = append(rev, diffOp{opEq, a[x-1]})
			x--
			y--
		}
		if d > 0 {
			if x == px {
				rev = append(rev, diffOp{opIns, b[y-1]})
			} else {
				rev = append(rev, diffOp{opDel, a[x-1]})
			}
			x, y = px, py
		}
	}
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev
}

func replaceAll(a, b []string) []diffOp {
	ops := make([]diffOp, 0, len(a)+len(b))
	for _, l := range a {
		ops = append(ops, diffOp{opDel, l})
	}
	for _, l := range b {
		ops = append(ops, diffOp{opIns, l})
	}
	return ops
}

// fileDiff is one file's diff in both forms: raw (counted against
// MaxFixDiffBytes and scanned for added text) and redacted (shown).
type fileDiff struct {
	FileDiff
	rawBytes int
	added    []string // the added lines, raw, without terminators
}

// unified renders the diff of old to new for path; isNew renders it as an
// added file. The shown text masks each line with redact.Text, and every line
// of a PEM private key block, so a secret already in the repository never
// reaches the room log.
func unified(path string, old, cur string, isNew bool) fileDiff {
	ops := diffLines(splitLines(old), splitLines(cur))
	var raw, shown strings.Builder
	fd := fileDiff{FileDiff: FileDiff{Path: path, Status: StatusModified}}
	from := "a/" + path
	if isNew {
		fd.Status = StatusAdded
		from = "/dev/null"
	}
	head := fmt.Sprintf("--- %s\n+++ b/%s\n", from, path)
	raw.WriteString(head)
	shown.WriteString(head)
	m := newMasker()
	for _, h := range hunks(ops) {
		hd := fmt.Sprintf("@@ -%s +%s @@\n", span(h.aStart, h.aLen), span(h.bStart, h.bLen))
		raw.WriteString(hd)
		shown.WriteString(hd)
		for _, op := range h.ops {
			text := op.text
			nl := strings.HasSuffix(text, "\n")
			body := strings.TrimSuffix(text, "\n")
			raw.WriteByte(byte(op.kind))
			raw.WriteString(body)
			raw.WriteByte('\n')
			shown.WriteByte(byte(op.kind))
			shown.WriteString(m.mask(body))
			shown.WriteByte('\n')
			if !nl {
				raw.WriteString(noNewline + "\n")
				shown.WriteString(noNewline + "\n")
			}
			switch op.kind {
			case opIns:
				fd.Added++
				fd.added = append(fd.added, body)
			case opDel:
				fd.Removed++
			}
		}
	}
	fd.Unified = shown.String()
	fd.rawBytes = raw.Len()
	return fd
}

// span is a hunk range: "start,len", or "start" when len is 1; an empty side
// starts at the line before it, as git writes it.
func span(start, n int) string {
	if n == 0 {
		return fmt.Sprintf("%d,0", start-1)
	}
	if n == 1 {
		return fmt.Sprint(start)
	}
	return fmt.Sprintf("%d,%d", start, n)
}

type hunk struct {
	aStart, aLen, bStart, bLen int
	ops                        []diffOp
}

// hunks groups the changes of ops with diffContext lines around each, joining
// two groups whose context would touch.
func hunks(ops []diffOp) []hunk {
	var out []hunk
	n := len(ops)
	i := 0
	aLine, bLine := 1, 1
	// pos[i] is the (a, b) line number of ops[i].
	posA, posB := make([]int, n), make([]int, n)
	for j, op := range ops {
		posA[j], posB[j] = aLine, bLine
		if op.kind != opIns {
			aLine++
		}
		if op.kind != opDel {
			bLine++
		}
	}
	for i < n {
		for i < n && ops[i].kind == opEq {
			i++
		}
		if i == n {
			break
		}
		start := max(0, i-diffContext)
		end := i
		for {
			for end < n && ops[end].kind != opEq {
				end++
			}
			// end is the first equal line after a change; look for the next change.
			next := end
			for next < n && ops[next].kind == opEq {
				next++
			}
			if next < n && next-end <= 2*diffContext {
				end = next
				continue
			}
			end = min(n, end+diffContext)
			break
		}
		h := hunk{aStart: posA[start], bStart: posB[start], ops: ops[start:end]}
		for _, op := range h.ops {
			if op.kind != opIns {
				h.aLen++
			}
			if op.kind != opDel {
				h.bLen++
			}
		}
		out = append(out, h)
		i = end
	}
	return out
}

// masker redacts shown lines one at a time, carrying the state of a PEM
// private key block across lines (a per-line redaction would miss it).
type masker struct{ inKey bool }

func newMasker() *masker { return &masker{} }

func (m *masker) mask(line string) string {
	if !m.inKey && strings.Contains(line, "-----BEGIN ") && strings.Contains(line, "PRIVATE KEY") {
		m.inKey = true
	}
	if m.inKey {
		if strings.Contains(line, "-----END ") {
			m.inKey = false
		}
		return "[redacted private key line]"
	}
	out, _ := redact.Text(line)
	return out
}
