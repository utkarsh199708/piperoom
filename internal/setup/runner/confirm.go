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

// ask prints prompt and reads one line from the runner's terminal, returning
// true on y/yes and false on anything else. The read happens on a goroutine
// and returns as soon as a line arrives or ctx ends, so a Ctrl-C stops the
// prompt (the caller returns ctx.Err()). The whole prompt+read is serialized
// under stdinMu and uses the one shared stdinReader, so two concurrent T3
// confirms cannot read each other's answer or have a reader that loses the
// race buffer (and drop) the other's input.
func (r *Runner) ask(ctx context.Context, prompt string) (bool, error) {
	r.stdinMu.Lock()
	defer r.stdinMu.Unlock()

	// An op whose ctx already ended has nothing to confirm; return before
	// printing a prompt or spawning a read goroutine.
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if r.stdinReader == nil {
		r.stdinReader = bufio.NewReader(r.stdin)
	}
	fmt.Fprint(r.stdout, prompt)

	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	// A blocked stdin read cannot be interrupted, so read on a goroutine and
	// select on ctx. If the person cancels, the read is abandoned; the process
	// is exiting, so the goroutine is reclaimed with it.
	go func() {
		line, err := r.stdinReader.ReadString('\n')
		ch <- result{line, err}
	}()

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
