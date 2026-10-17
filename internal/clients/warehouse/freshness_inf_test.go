package warehouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
)

// decodeRows returns one max() row and decodes it the way pgx does on the
// wire, so the tests exercise the real decoding of the destination type.
type decodeRows struct {
	fakeRows
	oid    uint32 // column type; timestamptz when 0
	text   *string
	binary []byte // used instead of text when set
	done   bool
}

func (r *decodeRows) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}

func (r *decodeRows) typ() uint32 {
	if r.oid == 0 {
		return pgtype.TimestamptzOID
	}
	return r.oid
}

func (r *decodeRows) FieldDescriptions() []pgconn.FieldDescription {
	return []pgconn.FieldDescription{{Name: "max", DataTypeOID: r.typ()}}
}

func (r *decodeRows) Scan(dest ...any) error {
	var err error
	if r.binary != nil {
		err = pgtype.NewMap().Scan(r.typ(), pgtype.BinaryFormatCode, r.binary, dest[0])
	} else {
		var src []byte
		if r.text != nil {
			src = []byte(*r.text)
		}
		err = pgtype.NewMap().Scan(r.typ(), pgtype.TextFormatCode, src, dest[0])
	}
	if err != nil {
		// pgx's own wrapping, which quotes the value it could not decode.
		return pgx.ScanArgError{ColumnIndex: 0, FieldName: "max", Err: err}
	}
	return nil
}

type decodeQuerier struct {
	fakeQuerier
	oid    uint32
	text   *string
	binary []byte
}

func (q *decodeQuerier) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return &decodeRows{oid: q.oid, text: q.text, binary: q.binary}, nil
}

// An 'infinity' max used to fail the scan, so the sink read as unreachable.
func TestFreshnessInfinity(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		name   string
		text   *string
		binary []byte // the wire format pgx uses by default: int64 microseconds
		future bool
		zero   bool
		want   time.Time // exact value when set
	}{
		{name: "infinity is bad data in the future", text: s("infinity"), future: true},
		{name: "-infinity is no usable value", text: s("-infinity"), zero: true},
		{name: "null is an empty table", text: nil, zero: true},
		{name: "normal value", text: s("2026-10-14 03:00:00+00"), want: time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)},
		{name: "binary infinity", binary: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, future: true},
		{name: "binary -infinity", binary: []byte{0x80, 0, 0, 0, 0, 0, 0, 0}, zero: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := testDB(&decodeQuerier{text: c.text, binary: c.binary})
			ts, err := d.Freshness(context.Background(), "analytics.orders", "loaded_at")
			if err != nil {
				t.Fatalf("Freshness: %v", err)
			}
			if ts.IsZero() != c.zero {
				t.Fatalf("zero = %v, want %v (ts %v)", ts.IsZero(), c.zero, ts)
			}
			if !c.want.IsZero() && !ts.Equal(c.want) {
				t.Fatalf("ts = %v, want %v", ts, c.want)
			}
			if c.future && !ts.After(time.Now().Add(365*24*time.Hour)) {
				t.Fatalf("infinity must read as far in the future, got %v", ts)
			}
		})
	}
}

// #564: a non-timestamp ts column must not leak max(col) through the error.
// The row description is checked before the value is decoded, and a decode
// error never quotes the value.
func TestFreshnessNeverQuotesValues(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		name  string
		oid   uint32
		value string
	}{
		{"text email", pgtype.TextOID, "zoe.secret@example.com"},
		{"varchar card number", pgtype.VarcharOID, "4111111111111111"},
		{"numeric amount", pgtype.NumericOID, "99.5"},
		{"bigint id", pgtype.Int8OID, "424242424242"},
		// A timestamptz column whose bytes cannot be decoded: pgx's error
		// quotes them, Freshness must not.
		{"undecodable timestamptz", pgtype.TimestamptzOID, "zoe.secret@example.com"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := testDB(&decodeQuerier{oid: c.oid, text: s(c.value)})
			_, err := d.Freshness(context.Background(), "analytics.orders", "email")
			if err == nil {
				t.Fatal("Freshness of a non-timestamp value succeeded")
			}
			if strings.Contains(err.Error(), c.value) {
				t.Fatalf("error quotes the value: %v", err)
			}
			if !errors.Is(err, ErrNotTimestamp) {
				t.Fatalf("err = %v, want ErrNotTimestamp", err)
			}
		})
	}
}

// A server data exception (class 22) can quote a value: it becomes a fixed
// phrase. A missing column keeps clients.ErrNotFound.
func TestFreshnessServerErrors(t *testing.T) {
	leak := &pgconn.PgError{Code: "22007", Message: `invalid input syntax for type timestamp with time zone: "zoe.secret@example.com"`}
	_, err := testDB(&fakeQuerier{err: leak}).Freshness(context.Background(), "analytics.orders", "email")
	if err == nil || strings.Contains(err.Error(), "zoe.secret") || !strings.Contains(err.Error(), "22007") {
		t.Fatalf("class 22 error = %v, want a fixed phrase with the SQLSTATE", err)
	}
	gone := &pgconn.PgError{Code: "42703", Message: `column "nope" does not exist`}
	if _, err := testDB(&fakeQuerier{err: gone}).Freshness(context.Background(), "analytics.orders", "nope"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("undefined column err = %v, want clients.ErrNotFound", err)
	}
}
