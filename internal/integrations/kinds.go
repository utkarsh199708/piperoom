package integrations

import (
	"context"
	"errors"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/integrations/spec"
)

// ErrUnsupported marks a Build that cannot work yet with the given values
// because a dependency lacks a feature (named in the error). The kind's Test
// reports the same gap as a check: fail when the saved row could never work
// (a pasted Postgres CA), warn when the server keeps its env setting meanwhile
// (the Anthropic key).
var ErrUnsupported = errors.New("integrations: not supported yet")

// Deps are what the built-in kinds need. The zero value works: no allow
// list, default deadlines.
type Deps struct {
	// Guard limits the hosts kinds dial. nil = NewGuard("", nil).
	Guard *Guard
	// CheckTimeout is the deadline of one check (default 5 s); TestTimeout of
	// a whole test (default 20 s).
	CheckTimeout time.Duration
	TestTimeout  time.Duration
	// KafkaRegistry returns the Schema Registry client the Kafka client uses
	// to decode Avro keys (kafka.New takes a concrete *schemareg.Client).
	// nil, or a nil result, gives the Kafka client a registry stub that
	// answers "Schema Registry is not connected".
	KafkaRegistry func() *schemareg.Client

	// Test seams; nil = the real implementation.
	pgConnect     pgConnector
	kafkaOpen     kafkaOpener
	sfOpen        sfOpener
	sfPort        string
	sfNew         func(ctx context.Context, dsn string) (any, func(), error)
	slackBase     string
	anthropicBase string
}

func (d Deps) guard() *Guard {
	if d.Guard != nil {
		return d.Guard
	}
	g, _ := NewGuard("", nil) // "" cannot fail
	return g
}

func (d Deps) dial() dialFunc { return d.guard().DialContext }

func (d Deps) checkTimeout() time.Duration {
	if d.CheckTimeout > 0 {
		return d.CheckTimeout
	}
	return DefaultCheckTimeout
}

func (d Deps) withDefaults() Deps {
	if d.Guard == nil {
		d.Guard = d.guard()
	}
	if d.pgConnect == nil {
		d.pgConnect = realPGConnect(d.Guard)
	}
	if d.kafkaOpen == nil {
		d.kafkaOpen = realKafkaOpen
	}
	if d.sfOpen == nil {
		d.sfOpen = realSnowflakeOpen
	}
	if d.slackBase == "" {
		d.slackBase = "https://slack.com/api"
	}
	return d
}

// BuiltinKinds returns the kinds this package ships, in catalog order
// within each category.
func BuiltinKinds(d Deps) []spec.Kind {
	d = d.withDefaults()
	var out []spec.Kind
	out = append(out, d.postgresKinds()...)
	out = append(out, d.kafkaKinds()...)
	out = append(out, d.connectKind(), d.registryKind())
	out = append(out, d.snowflakeKind())
	out = append(out, d.anthropicKind())
	out = append(out, alertKinds()...)
	out = append(out, d.slackKind())
	return out
}

// NewBuiltinRegistry returns a registry holding BuiltinKinds.
func NewBuiltinRegistry(d Deps) (*Registry, error) {
	r := NewRegistry()
	for _, k := range BuiltinKinds(d) {
		if err := r.Register(k); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// scrubErr replaces any secret value inside err's text. Drivers sometimes
// echo a DSN or a password in an error; this is the last line of defence.
func scrubErr(err error, secrets ...netsec.Secret) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	changed := false
	for _, s := range secrets {
		if r := s.Reveal(); len(r) >= 4 && strings.Contains(msg, r) {
			msg = strings.ReplaceAll(msg, r, "[redacted]")
			changed = true
		}
	}
	if !changed {
		return err
	}
	return &scrubbedError{msg: msg, err: err}
}

type scrubbedError struct {
	msg string
	err error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.err }

// splitList splits a comma list, trimming blanks.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
