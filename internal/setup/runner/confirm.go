package runner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
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

// confirm prints what the server claims the room is (origin, room, pipeline,
// tenant, creator, the targets the setup may reach with their classes, and
// the pairing fingerprint) and asks for a y/N on the runner's terminal,
// unless the runner was started with --yes, which Run only allows on
// loopback. It returns true on "y". Every field is untrusted server text: it
// is scrubbed before printing. The read happens on a goroutine and returns as
// soon as a line arrives or ctx ends, so a Ctrl-C stops the prompt (Run
// returns ctx.Err()). Nothing of the room's targets runs before this answer:
// pair registers the target drivers (and so loads any cloud credential
// chain) only after a "y".
func (r *Runner) confirm(ctx context.Context, welcome *protocol.Welcome) (bool, error) {
	creator := welcome.Creator.Name
	if creator == "" {
		creator = welcome.Creator.ID
	}
	if r.extConfirm != nil {
		return r.confirmExternal(ctx, welcome, creator)
	}
	r.printWelcome(welcome, creator)
	switch {
	case welcome.Fingerprint == "":
		fmt.Fprintln(r.stdout, "the server sent no fingerprint: compare the origin above with the address you opened the room on")
	case welcome.PublicOrigin == "":
		// The fingerprint hashes the server's PIPEROOM_PUBLIC_URL; without
		// one it binds the room but not the server's address.
		fmt.Fprintln(r.stdout, "the server has no PIPEROOM_PUBLIC_URL, so the fingerprint does not bind its address: check the origin above too")
	default:
		fmt.Fprintln(r.stdout, "compare the fingerprint with the one the room was opened with before you answer")
	}
	if r.yes {
		return true, nil
	}
	return r.ask(ctx, "continue? [y/N] ")
}

// printWelcome is the block both confirmations show: the origin the runner
// dialed, the room, its pipeline and tenant, the creator, each target (kind,
// address: the account, environment or project of a managed one, and class)
// and the fingerprint the Set up card shows.
func (r *Runner) printWelcome(welcome *protocol.Welcome, creator string) {
	fmt.Fprintf(r.stdout, "pair with %s\n", r.serverURL)
	fmt.Fprintf(r.stdout, "room %s: %s\n", scrub(welcome.RoomID, maxPrintedFieldLen), scrub(welcome.Title, maxPrintedFieldLen))
	fmt.Fprintf(r.stdout, "pipeline %s, tenant %s\n", scrub(welcome.PipelineID, maxPrintedFieldLen), scrub(welcome.Tenant, maxPrintedFieldLen))
	fmt.Fprintf(r.stdout, "created by %s\n", scrub(creator, maxPrintedFieldLen))
	for _, t := range welcome.Targets {
		fmt.Fprintf(r.stdout, "target %s %s (%s)\n", scrub(t.Kind, 32), scrub(t.Address, maxPrintedFieldLen), scrub(t.Class, 32))
	}
	fmt.Fprintf(r.stdout, "fingerprint %s\n", scrub(welcome.Fingerprint, 32))
}

// ErrOriginMismatch is a Welcome whose PublicOrigin is not the origin the
// runner was started with: the person would compare a fingerprint for a
// server they did not mean.
var ErrOriginMismatch = errors.New("setup: the server's public origin is not --server")

// errExternalConfirm marks an Options.Confirm failure: terminal, because a
// refused confirmation (a pre-existing or wrong confirm Secret) must not be
// retried into a second pairing attempt.
var errExternalConfirm = errors.New("setup: confirmation")

// confirmExternal is confirm for a runner without a terminal: the Welcome
// must name the origin the runner dialed as its public origin, the block is
// printed when the confirmer says so (after its own pre-checks), and the
// confirmer's answer decides.
func (r *Runner) confirmExternal(ctx context.Context, welcome *protocol.Welcome, creator string) (bool, error) {
	if strings.TrimRight(welcome.PublicOrigin, "/") != strings.TrimRight(r.serverURL, "/") {
		return false, fmt.Errorf("%w: welcome says %q, runner dialed %q", ErrOriginMismatch,
			scrub(welcome.PublicOrigin, maxPrintedFieldLen), r.serverURL)
	}
	show := func() { r.printWelcome(welcome, creator) }
	ok, err := r.extConfirm(ctx, welcome, show)
	if err != nil && ctx.Err() == nil {
		return false, fmt.Errorf("%w: %w", errExternalConfirm, err)
	}
	return ok, err
}

// stdinLine is one line (or the reader's terminal error) from the runner's
// terminal, as the single stdin-reader goroutine hands it to a prompt, with
// the (monotonic, wall) time the reader got it.
type stdinLine struct {
	line string
	err  error
	at   time.Time
}

// stdinCh returns the channel the runner's one stdin-reader goroutine feeds,
// starting that goroutine on first use. stdinMu must be held. Exactly one
// goroutine ever reads r.stdin: a prompt whose ctx ends abandons only its
// channel receive, never a ReadString, so a later prompt cannot race a live
// orphaned read on the same reader (a data race). A line typed for an
// abandoned prompt (a T3 confirmation whose op deadline passed) waits in this
// channel; nextLine discards it rather than let it answer the next prompt,
// which it was never typed for. After a read error (EOF included) the
// goroutine hands the same terminal result to every later prompt, exactly as
// a fresh ReadString on an exhausted reader would.
func (r *Runner) stdinCh() <-chan stdinLine {
	if r.stdinLines == nil {
		ch := make(chan stdinLine)
		r.stdinLines = ch
		reader := bufio.NewReader(r.stdin)
		go func() {
			for {
				line, err := reader.ReadString('\n')
				ch <- stdinLine{line: line, err: err, at: time.Now()}
				if err != nil {
					for {
						ch <- stdinLine{err: err, at: time.Now()}
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
	// printing a prompt. Its op may already have been announced (and the
	// person may be typing a "y" for it), so a line read before the next
	// prompt is shown is discarded, as for an abandoned prompt. The reader
	// is started now so such a line is stamped when it arrives, not when the
	// next prompt first reads it.
	select {
	case <-ctx.Done():
		r.stdinAbandoned = true
		r.stdinCh()
		return false, ctx.Err()
	default:
	}

	shown := time.Now()
	fmt.Fprint(r.stdout, prompt)

	res, err := r.nextLine(ctx, shown)
	if err != nil {
		return false, err
	}
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

// nextLine returns this prompt's line, or ctx's error. When the previous
// prompt was abandoned (its ctx ended first: a T3 op whose deadline passed),
// a line the reader got before this prompt was printed (shown) was typed for
// that abandoned prompt: it is discarded, never taken as this prompt's answer,
// so a late "y" for an expired T3 op cannot confirm the next one. After an
// answered prompt, a line read ahead (piped answers) is kept. stdinMu must be
// held.
func (r *Runner) nextLine(ctx context.Context, shown time.Time) (stdinLine, error) {
	ch := r.stdinCh()
	for {
		select {
		case <-ctx.Done():
			r.stdinAbandoned = true
			return stdinLine{}, ctx.Err()
		case res := <-ch:
			if r.stdinAbandoned && res.at.Before(shown) {
				continue
			}
			r.stdinAbandoned = false
			return res, nil
		}
	}
}
