package agent

import (
	"crypto/rand"
	"encoding/binary"
	"math/big"
	"time"
)

// crockford is the Crockford base32 alphabet ULIDs use (no I, L, O, U).
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID returns a ULID-compatible identifier: a 48-bit Unix-millisecond
// timestamp followed by 80 random bits, encoded as 26 Crockford base32 chars.
// The randomness comes from crypto/rand, so no global state is needed.
func newULID() string {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint16(b[0:2], uint16(ms>>32))
	binary.BigEndian.PutUint32(b[2:6], uint32(ms))
	if _, err := rand.Read(b[6:]); err != nil {
		// crypto/rand must never fail on a healthy system; fall back to
		// time-derived bits so we still return a well-formed id.
		now := uint64(time.Now().UnixNano())
		binary.BigEndian.PutUint64(b[6:14], now)
		binary.BigEndian.PutUint16(b[14:16], uint16(now>>56))
	}
	return ulidString(b)
}

// newStepID returns a room step id with the "s_" prefix used across Piperoom.
func newStepID() string {
	return "s_" + newULID()
}

// ulidString encodes 16 bytes (128 bits) as 26 base32 digits, most
// significant digit first. The top two bits are padding and always zero.
func ulidString(b [16]byte) string {
	n := new(big.Int).SetBytes(b[:])
	base := big.NewInt(int64(len(crockford)))
	var s [26]byte
	for i := len(s) - 1; i >= 0; i-- {
		var rem big.Int
		n.QuoRem(n, base, &rem)
		s[i] = crockford[rem.Int64()]
	}
	return string(s[:])
}
