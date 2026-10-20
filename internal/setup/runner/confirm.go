package runner

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"piperoom/internal/setup/protocol"
)

// ErrDeclined reports that the person answered N to the pairing question.
var ErrDeclined = errors.New("setup: pairing declined")

// confirm prints what the server claims the room is (origin, room, creator)
// and asks for a y/N on the runner's terminal, unless the runner was started
// with --yes, which Run only allows on loopback. It returns true on "y".
func (r *Runner) confirm(welcome *protocol.Welcome) (bool, error) {
	creator := welcome.Creator.Name
	if creator == "" {
		creator = welcome.Creator.ID
	}
	fmt.Fprintf(r.stdout, "pair with %s\n", r.serverURL)
	fmt.Fprintf(r.stdout, "room %s: %s\n", welcome.RoomID, welcome.Title)
	fmt.Fprintf(r.stdout, "created by %s\n", creator)
	if r.yes {
		return true, nil
	}
	fmt.Fprintf(r.stdout, "continue? [y/N] ")
	line, err := bufio.NewReader(r.stdin).ReadString('\n')
	if err != nil && err != io.EOF {
		return false, fmt.Errorf("setup: reading answer: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}
