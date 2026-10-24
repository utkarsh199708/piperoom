package runner

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/gowebpki/jcs"

	"piperoom/internal/events"
	"piperoom/internal/jsonnum"
)

// paramsHashVersion is the "v" member of the hashed object. It is kept in
// lock-step with internal/room (params_hash.go): the runner recomputes
// params_hash independently of the server, and any drift is caught by
// TestParamsHashMatchesRoom, which compares the two directly.
const paramsHashVersion = 2

// errParamsNumber mirrors internal/room's errParamsNumber: a proposal that
// holds a number a double cannot carry exactly cannot be hashed. The runner
// refuses such a proposal (RefuseParamsHash) rather than hash a value that
// differs from what the approver saw.
var errParamsNumber = errors.New("a number in the proposal cannot be written exactly as a double (an integer above 2^53 or a value outside the double range); write large integers such as offsets as strings")

// hashedProposal is the object params_hash covers, identical to internal/room's
// (internal/room/params_hash.go). Member order does not matter: JCS sorts keys.
type hashedProposal struct {
	V              int                   `json:"v"`
	RoomID         string                `json:"room_id"`
	ProposalID     string                `json:"proposal_id"`
	Title          string                `json:"title"`
	Risk           string                `json:"risk"`
	Rollback       string                `json:"rollback"`
	RequiredLayers []string              `json:"required_layers"`
	Steps          []events.ProposalStep `json:"steps"`
}

// ComputeParamsHash returns the hex SHA-256 of the RFC 8785 (JCS) form of the
// section 3.1 object for a proposal, exactly as internal/room.ComputeParamsHash
// does (docs/design/auth.md §3.1). The runner calls it to recompute the hash
// over the frozen proposal it was shown and compares the result with
// OpRequest.ParamsHash before it executes a T1+ op.
func ComputeParamsHash(roomID, proposalID, title, risk, rollback string, requiredLayers []string, steps []events.ProposalStep) (string, error) {
	if steps == nil {
		steps = []events.ProposalStep{}
	}
	raw, err := json.Marshal(hashedProposal{
		V: paramsHashVersion, RoomID: roomID, ProposalID: proposalID,
		Title: title, Risk: risk, Rollback: rollback,
		RequiredLayers: nonNilStrings(requiredLayers), Steps: steps,
	})
	if err != nil {
		return "", fmt.Errorf("setup: marshal proposal for hash: %w", err)
	}
	form, inexact := hashForm(raw)
	if inexact > 0 {
		return "", errParamsNumber
	}
	canon, err := jcs.Transform(form)
	if err != nil {
		return "", fmt.Errorf("setup: canonicalize proposal for hash: %w", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// canonicalParams returns the canonical form two params are compared in: the
// RFC 8785 (JCS) form of the params after the number rewrite params_hash
// applies. A step's params are always exact (a proposal holding a number a
// double cannot carry was already refused by the hash check), so a request
// whose params contain an inexact number is simply a mismatch.
func canonicalParams(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, fmt.Errorf("setup: compact params: %w", err)
	}
	form, inexact := hashForm(buf.Bytes())
	if inexact > 0 {
		return nil, errParamsNumber
	}
	canon, err := jcs.Transform(form)
	if err != nil {
		return nil, fmt.Errorf("setup: canonicalize params: %w", err)
	}
	return canon, nil
}

// The number handling below is copied from internal/room/numbers.go and must
// stay identical to it: params_hash covers the exact value the approver saw,
// so the runner and the room must classify a number token the same way.

// maxExactSigDigits caps the significant digits of a number that is compared
// exactly with big.Rat; a number with more is classified inexact.
const maxExactSigDigits = 800

// numberClass says how a number token fares as a float64.
type numberClass int

const (
	numberExact      numberClass = iota // a double holds it exactly (as JCS will write it)
	numberInexact                       // finite, but JCS writes a different value
	numberOutOfRange                    // not finite as float64; JCS cannot write it
	numberIgnore                        // does not parse as a number; ignore it
)

// classifyNumber classifies one number token. The decision is made on the
// significant form of the token (jsonnum.Significant), never on the raw text.
func classifyNumber(tok string) numberClass {
	digits, exp10, ok := jsonnum.Significant(tok)
	if !ok {
		return numberIgnore
	}
	if digits == "0" {
		return numberExact
	}
	mag := exp10 + len(digits)
	if mag > jsonnum.MaxMag {
		return numberOutOfRange
	} else if mag < jsonnum.MinMag {
		return numberInexact
	}
	sign := ""
	if tok[0] == '-' {
		sign = "-"
	}
	if len(digits) > maxExactSigDigits {
		top := sign + digits[:maxExactSigDigits] + "e" + strconv.Itoa(mag-maxExactSigDigits)
		if _, err := strconv.ParseFloat(top, 64); errors.Is(err, strconv.ErrRange) {
			return numberOutOfRange
		}
		return numberInexact
	}
	norm := sign + digits + "e" + strconv.Itoa(exp10)
	f, err := strconv.ParseFloat(norm, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return numberOutOfRange
		}
		return numberIgnore
	}
	if f == 0 {
		return numberInexact
	}
	want, ok := new(big.Rat).SetString(norm)
	if !ok {
		return numberInexact
	}
	written, err := jcs.NumberToJSON(f)
	if err != nil {
		return numberOutOfRange
	}
	got, ok := new(big.Rat).SetString(written)
	if !ok || got.Cmp(want) != 0 {
		return numberInexact
	}
	return numberExact
}

// hashForm returns exact (compact JSON) rewritten for the hash: every number a
// double cannot hold finitely is replaced by a JSON string with its exact text.
// inexact counts the numbers whose hashed form differs from the exact value.
func hashForm(exact []byte) (out []byte, inexact int) {
	// Capacity len(exact): append grows the slice for the few quoted numbers.
	out = make([]byte, 0, len(exact))
	last := 0
	jsonnum.Scan(exact, func(s, e int) {
		switch classifyNumber(string(exact[s:e])) {
		case numberInexact:
			inexact++
		case numberOutOfRange:
			inexact++
			out = append(out, exact[last:s]...)
			out = append(out, '"')
			out = append(out, exact[s:e]...)
			out = append(out, '"')
			last = e
		}
	})
	if last == 0 && inexact == 0 {
		return exact, 0
	}
	out = append(out, exact[last:]...)
	return out, inexact
}
