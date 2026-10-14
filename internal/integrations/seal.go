package integrations

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// keyLen is the AES-256 key size PIPEROOM_SECRET_KEY must decode to.
const keyLen = 32

// ErrBadKey marks a PIPEROOM_SECRET_KEY (or _PREVIOUS) that is not 32 bytes of
// standard base64. The error never contains the key.
var ErrBadKey = errors.New("integrations: secret key must be 32 bytes, standard base64")

// ErrOpen marks a sealed blob that no configured key opens: a wrong key, a
// blob copied onto another row (additional data mismatch) or corruption.
var ErrOpen = errors.New("integrations: cannot open sealed secrets")

// sealKey is one AES-256 key and its id.
type sealKey struct {
	id   string
	aead cipher.AEAD
}

// Keyring seals typed-in secret values with the current key and opens them
// with the current or the previous key (rotation, integrations.md §4.3). The
// zero value is not usable; build one with NewKeyring.
type Keyring struct {
	cur  sealKey
	prev *sealKey
}

// NewKeyring parses PIPEROOM_SECRET_KEY and PIPEROOM_SECRET_KEY_PREVIOUS
// (standard base64, 32 bytes each). current "" returns (nil, nil): no key is
// configured, and every typed-in secret write answers spec.ErrSecretKeyMissing.
// A previous key without a current key is an error.
func NewKeyring(current, previous netsec.Secret) (*Keyring, error) {
	if current == "" {
		if previous != "" {
			return nil, fmt.Errorf("%w: PIPEROOM_SECRET_KEY_PREVIOUS is set but PIPEROOM_SECRET_KEY is not", ErrBadKey)
		}
		return nil, nil
	}
	cur, err := parseKey(current)
	if err != nil {
		return nil, fmt.Errorf("integrations: PIPEROOM_SECRET_KEY: %w", err)
	}
	kr := &Keyring{cur: cur}
	if previous != "" {
		p, err := parseKey(previous)
		if err != nil {
			return nil, fmt.Errorf("integrations: PIPEROOM_SECRET_KEY_PREVIOUS: %w", err)
		}
		if p.id != cur.id {
			kr.prev = &p
		}
	}
	return kr, nil
}

func parseKey(s netsec.Secret) (sealKey, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s.Reveal()))
	if err != nil || len(raw) != keyLen {
		// The decode error can quote input bytes, so it is dropped.
		return sealKey{}, ErrBadKey
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return sealKey{}, ErrBadKey
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return sealKey{}, ErrBadKey
	}
	return sealKey{id: KeyID(raw), aead: aead}, nil
}

// KeyID is the first 8 hex characters of SHA-256 of the raw key.
func KeyID(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:8]
}

// CurrentKeyID names the key Seal uses.
func (k *Keyring) CurrentKeyID() string { return k.cur.id }

// SealAAD is the additional data that binds a sealed blob to its row:
// "<workspace_id>/<id>/<category>".
func SealAAD(workspaceID, id string, cat spec.Category) []byte {
	return []byte(workspaceID + "/" + id + "/" + string(cat))
}

// Seal encrypts the secret values as a JSON object with AES-256-GCM under the
// current key. It returns nonce||ciphertext and the key id. An empty map seals
// to (nil, "", nil): the row has no typed-in secrets.
func (k *Keyring) Seal(aad []byte, secrets map[string]netsec.Secret) ([]byte, string, error) {
	if len(secrets) == 0 {
		return nil, "", nil
	}
	plain := make(map[string]string, len(secrets))
	for name, v := range secrets {
		plain[name] = v.Reveal()
	}
	js, err := json.Marshal(plain)
	if err != nil {
		return nil, "", fmt.Errorf("integrations: sealing: %w", err)
	}
	defer clear(js)
	nonce := make([]byte, k.cur.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, "", fmt.Errorf("integrations: sealing nonce: %w", err)
	}
	out := k.cur.aead.Seal(nonce, nonce, js, aad)
	return out, k.cur.id, nil
}

// Open decrypts a blob made by Seal. keyID picks the key; an unknown id (or
// "") tries every configured key. nil sealed opens to an empty map.
func (k *Keyring) Open(aad, sealed []byte, keyID string) (map[string]netsec.Secret, error) {
	if len(sealed) == 0 {
		return map[string]netsec.Secret{}, nil
	}
	var keys []sealKey
	switch {
	case keyID == k.cur.id:
		keys = []sealKey{k.cur}
	case k.prev != nil && keyID == k.prev.id:
		keys = []sealKey{*k.prev}
	default:
		keys = []sealKey{k.cur}
		if k.prev != nil {
			keys = append(keys, *k.prev)
		}
	}
	for _, key := range keys {
		ns := key.aead.NonceSize()
		if len(sealed) < ns+key.aead.Overhead() {
			return nil, fmt.Errorf("%w: blob too short", ErrOpen)
		}
		js, err := key.aead.Open(nil, sealed[:ns], sealed[ns:], aad)
		if err != nil {
			continue
		}
		var plain map[string]string
		err = json.Unmarshal(js, &plain)
		clear(js)
		if err != nil {
			return nil, fmt.Errorf("%w: bad JSON inside", ErrOpen)
		}
		out := make(map[string]netsec.Secret, len(plain))
		for name, v := range plain {
			out[name] = netsec.Secret(v)
		}
		return out, nil
	}
	return nil, ErrOpen
}

// sortedKeys returns the keys of m, sorted.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
