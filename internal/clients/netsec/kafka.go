package netsec

import (
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Kafka security protocols. The names match Kafka's own security.protocol
// setting so a customer can copy them from their client.properties.
const (
	Plaintext     = "PLAINTEXT"
	SSL           = "SSL"
	SASLPlaintext = "SASL_PLAINTEXT"
	SASLSSL       = "SASL_SSL"
)

// SASL mechanisms, named as in Kafka's sasl.mechanism setting.
const (
	MechPlain       = "PLAIN"
	MechSCRAMSHA256 = "SCRAM-SHA-256"
	MechSCRAMSHA512 = "SCRAM-SHA-512"
)

// dialTimeout matches franz-go's default; DialTLSConfig would otherwise also
// use 10 s, but saying it here keeps the deadline visible (AGENTS.md rule 4).
const dialTimeout = 10 * time.Second

// Kafka holds broker connection security.
type Kafka struct {
	// Protocol is PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL. Empty means
	// PLAINTEXT.
	Protocol string
	// Mechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. Required for the
	// SASL_* protocols, must be empty otherwise.
	Mechanism string
	Username  string
	Password  Secret
	// TLS is used by SSL and SASL_SSL.
	TLS TLS
}

// Opts returns the franz-go options for these settings. They are meant to be
// passed to every kgo.NewClient that talks to the same cluster.
func (k Kafka) Opts() ([]kgo.Opt, error) {
	proto := strings.ToUpper(strings.TrimSpace(k.Protocol))
	if proto == "" {
		proto = Plaintext
	}
	var useTLS, useSASL bool
	switch proto {
	case Plaintext:
	case SSL:
		useTLS = true
	case SASLPlaintext:
		useSASL = true
	case SASLSSL:
		useTLS, useSASL = true, true
	default:
		return nil, fmt.Errorf("%w: kafka security protocol %q (want %s, %s, %s or %s)",
			ErrInvalid, k.Protocol, Plaintext, SSL, SASLPlaintext, SASLSSL)
	}

	// Settings that the chosen protocol would ignore are an error: silently
	// dropping a password or CA usually means the protocol is wrong.
	if !useTLS && !k.TLS.IsZero() {
		return nil, fmt.Errorf("%w: kafka TLS files are set but security protocol is %s", ErrInvalid, proto)
	}
	if !useSASL && (k.Mechanism != "" || k.Username != "" || k.Password != "") {
		return nil, fmt.Errorf("%w: kafka SASL login is set but security protocol is %s", ErrInvalid, proto)
	}

	opts := []kgo.Opt{kgo.DialTimeout(dialTimeout)}
	if useTLS {
		cfg, err := k.TLS.Config()
		if err != nil {
			return nil, fmt.Errorf("netsec: kafka TLS: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	if useSASL {
		mech, err := k.mechanism(useTLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

func (k Kafka) mechanism(useTLS bool) (sasl.Mechanism, error) {
	if k.Username == "" || k.Password == "" {
		return nil, fmt.Errorf("%w: kafka SASL needs a username and a password", ErrInvalid)
	}
	switch strings.ToUpper(strings.TrimSpace(k.Mechanism)) {
	case MechPlain:
		// PLAIN sends the password as-is; without TLS anyone on the path
		// reads it. SCRAM never sends the password, so it may run without TLS.
		if !useTLS {
			return nil, fmt.Errorf("%w: SASL PLAIN without TLS sends the password in clear text; use %s", ErrInvalid, SASLSSL)
		}
		return plain.Auth{User: k.Username, Pass: k.Password.Reveal()}.AsMechanism(), nil
	case MechSCRAMSHA256:
		return scram.Auth{User: k.Username, Pass: k.Password.Reveal()}.AsSha256Mechanism(), nil
	case MechSCRAMSHA512:
		return scram.Auth{User: k.Username, Pass: k.Password.Reveal()}.AsSha512Mechanism(), nil
	case "":
		return nil, fmt.Errorf("%w: kafka SASL mechanism is required (%s, %s or %s)",
			ErrInvalid, MechPlain, MechSCRAMSHA256, MechSCRAMSHA512)
	default:
		return nil, fmt.Errorf("%w: kafka SASL mechanism %q (want %s, %s or %s)",
			ErrInvalid, k.Mechanism, MechPlain, MechSCRAMSHA256, MechSCRAMSHA512)
	}
}
