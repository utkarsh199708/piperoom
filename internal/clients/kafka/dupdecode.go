package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/hamba/avro/v2"
)

// Decoding source.lsn out of an untrusted change event value (DupScan, X1c).
// A record value can be attacker-controlled, so it is never handed to a
// generic decoder: hamba's skip decoders loop on a block count without
// checking it against the bytes left. The walker below reads the Avro
// binary itself, against the registry's writer schema, and every loop is
// bounded by the bytes of the value.

const (
	// maxValueSchemaBytes caps a value schema's text before parsing; a wide
	// Debezium table's envelope is far smaller.
	maxValueSchemaBytes = 512 << 10
	// maxAvroDepth bounds the walker's recursion (recursive record types).
	maxAvroDepth = 64
	// maxAvroOps bounds the schema nodes the walker visits for one value: a
	// record type built of empty records nested in pairs encodes to no
	// bytes yet doubles the walk at each level. A real envelope needs a
	// few per column.
	maxAvroOps = 1 << 20
)

var errAvro = errors.New("malformed avro")

// dbzEnvelope is a value schema DupScan checked: root is the envelope
// record, or why says why the schema's records are undecodable.
type dbzEnvelope struct {
	root *avro.RecordSchema
	why  string
}

// jsonLSN reads source.lsn from a JsonConverter value, with
// (schemas.enable) or without the schema/payload wrapper.
func jsonLSN(v []byte) (int64, string) {
	type source struct {
		LSN json.RawMessage `json:"lsn"`
	}
	type body struct {
		Source *source `json:"source"`
	}
	var env struct {
		body
		Payload *body `json:"payload"`
	}
	i := 0
	for i < len(v) && (v[i] == ' ' || v[i] == '\t' || v[i] == '\r' || v[i] == '\n') {
		i++
	}
	if i == len(v) || v[i] != '{' {
		return 0, UndecodableFormat
	}
	if err := json.Unmarshal(v, &env); err != nil {
		return 0, UndecodableMalformed
	}
	src := env.Source
	if env.Payload != nil && env.Payload.Source != nil {
		src = env.Payload.Source
	}
	if src == nil || len(src.LSN) == 0 || string(src.LSN) == "null" {
		return 0, UndecodableNoLSN
	}
	n, err := strconv.ParseInt(string(src.LSN), 10, 64)
	if err != nil {
		return 0, UndecodableMalformed
	}
	return n, ""
}

// envelopeSchema returns the checked value schema for a Confluent schema id,
// cached by id like schemaByID (refusals too). down is the scan's registry
// state: once the registry failed, an uncached id is not fetched again.
func (c *Client) envelopeSchema(ctx context.Context, id int, down *error) (*dbzEnvelope, string) {
	c.srMu.Lock()
	env, ok := c.envCache[id]
	c.srMu.Unlock()
	if ok {
		return env, env.why
	}
	if c.sr == nil || (down != nil && *down != nil) {
		return nil, UndecodableRegistry
	}
	text, err := c.sr.SchemaByID(ctx, id)
	if err != nil {
		if !schemaNotFound(err) {
			if down != nil {
				*down = err
			}
			return nil, UndecodableRegistry
		}
		env = &dbzEnvelope{why: UndecodableSchema}
	} else {
		env = parseEnvelope(text)
	}
	c.srMu.Lock()
	defer c.srMu.Unlock()
	if c.envCache == nil {
		c.envCache = map[int]*dbzEnvelope{}
	}
	if len(c.envCache) < maxSchemaCache {
		c.envCache[id] = env
	}
	return env, env.why
}

// parseEnvelope checks that text is a Debezium envelope whose source record
// has an lsn field: a record with a "source" field that is (or has a union
// branch that is) a record with "lsn". Anything else is refused once here,
// not per record.
func parseEnvelope(text string) *dbzEnvelope {
	if len(text) > maxValueSchemaBytes {
		return &dbzEnvelope{why: UndecodableSchema}
	}
	// A fresh cache per schema, as in schemaByID.
	s, err := avro.ParseWithCache(text, "", &avro.SchemaCache{})
	if err != nil {
		return &dbzEnvelope{why: UndecodableSchema}
	}
	root, ok := deref(s).(*avro.RecordSchema)
	if !ok {
		return &dbzEnvelope{why: UndecodableNoLSN}
	}
	for _, f := range root.Fields() {
		if f.Name() != "source" {
			continue
		}
		for _, b := range branches(f.Type()) {
			if rec, ok := b.(*avro.RecordSchema); ok && fieldNamed(rec, "lsn") {
				return &dbzEnvelope{root: root}
			}
		}
	}
	return &dbzEnvelope{why: UndecodableNoLSN}
}

func fieldNamed(rec *avro.RecordSchema, name string) bool {
	for _, f := range rec.Fields() {
		if f.Name() == name {
			return true
		}
	}
	return false
}

// deref follows named-type references (bounded: a reference names a
// defined type, never another reference, but the bound costs nothing).
func deref(s avro.Schema) avro.Schema {
	for range maxAvroDepth {
		r, ok := s.(*avro.RefSchema)
		if !ok {
			return s
		}
		s = r.Schema()
	}
	return s
}

// branches is s's union branches, dereferenced, or s itself.
func branches(s avro.Schema) []avro.Schema {
	s = deref(s)
	u, ok := s.(*avro.UnionSchema)
	if !ok {
		return []avro.Schema{s}
	}
	out := make([]avro.Schema, 0, len(u.Types()))
	for _, t := range u.Types() {
		out = append(out, deref(t))
	}
	return out
}

// lsn walks one Avro-encoded envelope (after the 5-byte Confluent header)
// to source.lsn, skipping the fields before it; what follows is not read.
func (e *dbzEnvelope) lsn(b []byte) (int64, string) {
	r := &avroCur{b: b}
	for _, f := range e.root.Fields() {
		if f.Name() != "source" {
			if err := r.skip(f.Type(), 0); err != nil {
				return 0, UndecodableMalformed
			}
			continue
		}
		src, err := r.branch(f.Type())
		if err != nil {
			return 0, UndecodableMalformed
		}
		rec, ok := src.(*avro.RecordSchema)
		if !ok {
			return 0, UndecodableNoLSN // a null source
		}
		for _, sf := range rec.Fields() {
			if sf.Name() != "lsn" {
				if err := r.skip(sf.Type(), 0); err != nil {
					return 0, UndecodableMalformed
				}
				continue
			}
			t, err := r.branch(sf.Type())
			if err != nil {
				return 0, UndecodableMalformed
			}
			if t.Type() != avro.Long && t.Type() != avro.Int {
				return 0, UndecodableNoLSN // null, or not a position
			}
			n, err := r.long()
			if err != nil {
				return 0, UndecodableMalformed
			}
			return n, ""
		}
		return 0, UndecodableNoLSN
	}
	return 0, UndecodableNoLSN
}

// avroCur reads Avro binary from b. Every read checks the bytes left, and
// every schema node visited counts against maxAvroOps.
type avroCur struct {
	b   []byte
	i   int
	ops int
}

// visit counts one schema node; false once the budget is spent.
func (r *avroCur) visit() bool {
	r.ops++
	return r.ops <= maxAvroOps
}

func (r *avroCur) left() int { return len(r.b) - r.i }

// long reads a zig-zag varint (int and long share the encoding).
func (r *avroCur) long() (int64, error) {
	var u uint64
	for shift := uint(0); shift < 70; shift += 7 {
		if r.i >= len(r.b) {
			return 0, errAvro
		}
		c := r.b[r.i]
		r.i++
		u |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return int64(u>>1) ^ -int64(u&1), nil
		}
	}
	return 0, errAvro
}

func (r *avroCur) advance(n int64) error {
	if n < 0 || n > int64(r.left()) {
		return errAvro
	}
	r.i += int(n)
	return nil
}

// branch resolves s against the data: a union reads its branch index.
func (r *avroCur) branch(s avro.Schema) (avro.Schema, error) {
	s = deref(s)
	u, ok := s.(*avro.UnionSchema)
	if !ok {
		return s, nil
	}
	idx, err := r.long()
	if err != nil || idx < 0 || idx >= int64(len(u.Types())) {
		return nil, errAvro
	}
	return deref(u.Types()[idx]), nil
}

// skip moves past one value of schema s. A block of items is looped only
// when its count fits in the bytes left (each item of a non-empty type
// takes at least one byte), and never for an empty item type, so the work
// is bounded by len(b) times the schema's depth.
func (r *avroCur) skip(s avro.Schema, depth int) error {
	if depth > maxAvroDepth || !r.visit() {
		return errAvro
	}
	s, err := r.branch(s)
	if err != nil {
		return err
	}
	switch s.Type() {
	case avro.Null:
		return nil
	case avro.Boolean:
		return r.advance(1)
	case avro.Int, avro.Long, avro.Enum:
		_, err := r.long()
		return err
	case avro.Float:
		return r.advance(4)
	case avro.Double:
		return r.advance(8)
	case avro.String, avro.Bytes:
		return r.skipBytes()
	case avro.Fixed:
		return r.advance(int64(s.(*avro.FixedSchema).Size()))
	case avro.Record:
		for _, f := range s.(*avro.RecordSchema).Fields() {
			if err := r.skip(f.Type(), depth+1); err != nil {
				return err
			}
		}
		return nil
	case avro.Array:
		return r.blocks(s.(*avro.ArraySchema).Items(), false, depth)
	case avro.Map:
		return r.blocks(s.(*avro.MapSchema).Values(), true, depth)
	}
	return errAvro
}

// skipBytes moves past a length-prefixed string or bytes value.
func (r *avroCur) skipBytes() error {
	n, err := r.long()
	if err != nil {
		return err
	}
	return r.advance(n)
}

// blocks skips an array's or a map's blocks (a map entry is a string key
// then the value).
func (r *avroCur) blocks(item avro.Schema, isMap bool, depth int) error {
	empty := !isMap && r.zeroSize(item, depth+1)
	for {
		n, err := r.long()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if n < 0 {
			// A negative count is followed by the block's size in bytes.
			if n == -n {
				return errAvro
			}
			size, err := r.long()
			if err != nil {
				return err
			}
			if err := r.advance(size); err != nil {
				return err
			}
			continue
		}
		if empty {
			continue
		}
		if n > int64(r.left()) {
			return errAvro
		}
		for range n {
			if isMap {
				if err := r.skipBytes(); err != nil {
					return err
				}
			}
			if err := r.skip(item, depth+1); err != nil {
				return err
			}
		}
	}
}

// zeroSize reports whether every value of s encodes to no bytes (null, a
// zero-size fixed, a record of such fields). A union never does: its index
// takes a byte. Past the depth or the ops budget it says false: the block
// is then looped, bounded by the bytes left, and the next skip fails.
func (r *avroCur) zeroSize(s avro.Schema, depth int) bool {
	if depth > maxAvroDepth || !r.visit() {
		return false
	}
	switch t := deref(s).(type) {
	case *avro.PrimitiveSchema:
		return t.Type() == avro.Null
	case *avro.FixedSchema:
		return t.Size() == 0
	case *avro.RecordSchema:
		for _, f := range t.Fields() {
			if !r.zeroSize(f.Type(), depth+1) {
				return false
			}
		}
		return true
	}
	return false
}
