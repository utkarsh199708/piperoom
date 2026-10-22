package runner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"piperoom/internal/setup/protocol"
)

// ErrDeclined reports that the person answered N to the pairing question.
var ErrDeclined = errors.New("setup: pairing declined")

// maxPrintedFieldLen caps how much of one untrusted server field reaches the
// terminal or an error message.
const maxPrintedFieldLen = 200

// scrub makes untrusted server text safe to print: control characters (C0
// r < 0x20 and 0x7f), C1 controls (0x80-0x9f) and Unicode format characters
// (unicode.Cf, e.g. the bidi overrides) are written as their \xNN or \uNNNN
// escape instead of acting on the terminal, and the result is capped at maxLen
// runes. Everything else passes through as-is.
func scrub(s string, maxLen int) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f):
			fmt.Fprintf(&b, "\\x%02x", r)
		case unicode.Is(unicode.Cf, r):
			fmt.Fprintf(&b, "\\u%04x", r)
		default:
			b.WriteRune(r)
		}
	}
	out := []rune(b.String())
	if len(out) > maxLen {
		return string(out[:maxLen]) + "…"
	}
	return string(out)
}

// confirm prints what the server claims the room is (origin, room, creator)
// and asks for a y/N on the runner's terminal, unless the runner was started
// with --yes, which Run only allows on loopback. It returns true on "y". The
// room id, title and creator are untrusted server text: they are scrubbed
// before printing. The read happens on a goroutine and returns as soon as a
// line arrives or ctx ends, so a Ctrl-C stops the prompt (Run returns
// ctx.Err()).
func (r *Runner) confirm(ctx context.Context, welcome *protocol.Welcome) (bool, error) {
	creator := welcome.Creator.Name
	if creator == "" {
		creator = welcome.Creator.ID
	}
	fmt.Fprintf(r.stdout, "pair with %s\n", r.serverURL)
	fmt.Fprintf(r.stdout, "room %s: %s\n", scrub(welcome.RoomID, maxPrintedFieldLen), scrub(welcome.Title, maxPrintedFieldLen))
	fmt.Fprintf(r.stdout, "created by %s\n", scrub(creator, maxPrintedFieldLen))
	if r.yes {
		return true, nil
	}
	return r.ask(ctx, "continue? [y/N] ")
}

// stdinLine is one line (or the reader's terminal error) from the runner's
// terminal, as the single stdin-reader goroutine hands it to a prompt.
type stdinLine struct {
	line string
	err  error
}

// stdinCh returns the channel the runner's one stdin-reader goroutine feeds,
// starting that goroutine on first use. stdinMu must be held. Exactly one
// goroutine ever reads r.stdin: a prompt whose ctx ends abandons only its
// channel receive, never a ReadString, so a later prompt cannot race a live
// orphaned read on the same reader (a data race) and a line typed for an
// abandoned prompt is not swallowed by a dead channel — it answers the next
// prompt instead. After a read error (EOF included) the goroutine hands the
// same terminal result to every later prompt, exactly as a fresh ReadString
// on an exhausted reader would.
func (r *Runner) stdinCh() <-chan stdinLine {
	if r.stdinLines == nil {
		ch := make(chan stdinLine)
		r.stdinLines = ch
		reader := bufio.NewReader(r.stdin)
		go func() {
			for {
				line, err := reader.ReadString('\n')
				ch <- stdinLine{line: line, err: err}
				if err != nil {
					for {
						ch <- stdinLine{err: err}
					}
				}
			}
		}()
	}
	return r.stdinLines
}

// ask prints prompt and reads one line from the runner's terminal, returning
// true on y/yes and false on anything else. The line comes from the single
// stdin-reader goroutine (stdinCh) and arrives as soon as the person answers
// or ctx ends, so a Ctrl-C stops the prompt (the caller returns ctx.Err()).
// The whole prompt+read is serialized under stdinMu, so two concurrent T3
// confirms cannot read each other's answer.
func (r *Runner) ask(ctx context.Context, prompt string) (bool, error) {
	r.stdinMu.Lock()
	defer r.stdinMu.Unlock()

	// An op whose ctx already ended has nothing to confirm; return before
	// printing a prompt.
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	ch := r.stdinCh()
	fmt.Fprint(r.stdout, prompt)

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case res := <-ch:
		if res.err != nil && res.err != io.EOF {
			return false, fmt.Errorf("setup: reading answer: %w", res.err)
		}
		switch strings.ToLower(strings.TrimSpace(res.line)) {
		case "y", "yes":
			return true, nil
		default:
			return false, nil
		}
	}
}
