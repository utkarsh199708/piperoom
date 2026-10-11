package room

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// crockford is the Crockford base32 alphabet ULIDs use.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newID returns prefix + a ULID (48-bit millisecond time, 80 random bits,
// 26 Crockford base32 chars), e.g. "r_01J...". Time-ordered so ids sort by age.
func newID(prefix string, now time.Time) string {
	var b [16]byte
	ms := uint64(now.UnixMilli())
	binary.BigEndian.PutUint16(b[0:2], uint16(ms>>32))
	binary.BigEndian.PutUint32(b[2:6], uint32(ms))
	// crypto/rand does not fail on a healthy system; a short read would only
	// weaken uniqueness, which the store's primary key still enforces.
	_, _ = rand.Read(b[6:])
	// Encode the 128 bits as 26 digits of 5 bits (the top 2 bits are padding).
	var out [26]byte
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])
	for i := 25; i >= 0; i-- {
		out[i] = crockford[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return prefix + string(out[:])
}
