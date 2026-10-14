package room

import (
	"fmt"
	"unicode/utf8"
)

// Caps on text a person types. The 1 MB body limit alone lets one request put
// a megabyte title on every room list and every snapshot each watcher folds,
// so the room service refuses longer text up front with a 400 that names the
// limit. Lengths are in characters (runes), which is what the person sees.
const (
	// MaxTitleChars caps a room title (POST /api/rooms; alert titles are cut).
	MaxTitleChars = 200
	// MaxMessageChars caps a comment or steer text.
	MaxMessageChars = 4000
)

// checkLen refuses s when it is longer than limit characters. field names the
// input in the message so the client can show which one was too long; the
// text itself is never quoted.
func checkLen(field, s string, limit int) error {
	if n := utf8.RuneCountInString(s); n > limit {
		return validation(fmt.Sprintf("%s is too long: %d characters, the limit is %d", field, n, limit))
	}
	return nil
}

// clipTitle shortens an alert title to MaxTitleChars, ending it with "…".
// An alert is never refused for its title: a sender cannot be asked to retry
// with a shorter one, and dropping the alert would hide the incident.
func clipTitle(s string) string {
	if utf8.RuneCountInString(s) <= MaxTitleChars {
		return s
	}
	n := 0
	for i := range s {
		if n == MaxTitleChars-1 {
			return s[:i] + "…"
		}
		n++
	}
	return s
}
