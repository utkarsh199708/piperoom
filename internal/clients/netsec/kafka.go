package netsec

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
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
	// MechAWSMSKIAM is Amazon MSK's IAM login. It signs each SASL session
	// with AWS credentials instead of sending a username and password.
	MechAWSMSKIAM = "AWS_MSK_IAM"
)

// mskIAMUserAgent goes into the signed MSK IAM payload so a customer can match
// Piperoom's sessions with the aws:UserAgent IAM condition key.
const mskIAMUserAgent = "piperoom"

// dialTimeout matches franz-go's default; DialTLSConfig would otherwise also
// use 10 s, but saying it here keeps the deadline visible (AGENTS.md rule 4).
const dialTimeout = 10 * time.Second

// AWSCredentials are one set of AWS credentials for one SASL session.
type AWSCredentials struct{ AccessKey, SecretKey, SessionToken string }

// String hides the values so the credentials never reach a log or an error.
func (AWSCredentials) String() string { return "[redacted aws credentials]" }

// GoString hides the values for %#v.
func (c AWSCredentials) GoString() string { return c.String() }

// LogValue hides the values for log/slog.
func (c AWSCredentials) LogValue() slog.Value { return slog.StringValue(c.String()) }

// Kafka holds broker connection security.
type Kafka struct {
	// Protocol is PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL. Empty means
	// PLAINTEXT.
	Protocol string
	// Mechanism is PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or AWS_MSK_IAM.
	// Required for the SASL_* protocols, must be empty otherwise.
	Mechanism string
	Username  string
	Password  Secret
	// AWSCredentialsFunc supplies MSK IAM credentials per SASL session.
	// Required with Mechanism AWS_MSK_IAM, must be nil otherwise. It is a
	// func, not an AWS SDK type, so this package needs no AWS SDK import; the
	// wiring layer fills it from the process's one aws.Config.
	AWSCredentialsFunc func(ctx context.Context) (AWSCredentials, error)
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
	if !useSASL && (k.Mechanism != "" || k.Username != "" || k.Password != "" || k.AWSCredentialsFunc != nil) {
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
	mech := strings.ToUpper(strings.TrimSpace(k.Mechanism))
	// MSK IAM is checked first: it has no username or password, so the
	// login check below does not apply to it.
	if mech == MechAWSMSKIAM {
		return k.mskIAM(useTLS)
	}
	if k.AWSCredentialsFunc != nil {
		return nil, fmt.Errorf("%w: AWS credentials are set but kafka SASL mechanism is %q; they are used only with %s",
			ErrInvalid, k.Mechanism, MechAWSMSKIAM)
	}
	if k.Username == "" || k.Password == "" {
		return nil, fmt.Errorf("%w: kafka SASL needs a username and a password", ErrInvalid)
	}
	switch mech {
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
		return nil, fmt.Errorf("%w: kafka SASL mechanism is required (%s, %s, %s or %s)",
			ErrInvalid, MechPlain, MechSCRAMSHA256, MechSCRAMSHA512, MechAWSMSKIAM)
	default:
		return nil, fmt.Errorf("%w: kafka SASL mechanism %q (want %s, %s, %s or %s)",
			ErrInvalid, k.Mechanism, MechPlain, MechSCRAMSHA256, MechSCRAMSHA512, MechAWSMSKIAM)
	}
}

// mskIAM builds the AWS_MSK_IAM mechanism. franz-go asks for credentials and
// signs a fresh payload for every SASL session, so rotated role credentials
// are picked up without a restart. franz-go takes the signing region from an
// MSK broker host name, else from AWS_REGION / AWS_DEFAULT_REGION.
func (k Kafka) mskIAM(useTLS bool) (sasl.Mechanism, error) {
	// MSK serves IAM only on its TLS listener; over plaintext the login would
	// fail later with an opaque broker error, so refuse it here.
	if !useTLS {
		return nil, fmt.Errorf("%w: kafka SASL mechanism %s needs security protocol %s, not %s (MSK IAM listeners are TLS only)",
			ErrInvalid, MechAWSMSKIAM, SASLSSL, SASLPlaintext)
	}
	if k.Username != "" || k.Password != "" {
		return nil, fmt.Errorf("%w: kafka SASL mechanism %s uses AWS credentials; unset the kafka SASL username and password",
			ErrInvalid, MechAWSMSKIAM)
	}
	if k.AWSCredentialsFunc == nil {
		return nil, fmt.Errorf("%w: kafka SASL mechanism %s needs AWS credentials (default AWS chain, AWS_REGION)",
			ErrInvalid, MechAWSMSKIAM)
	}
	creds := k.AWSCredentialsFunc
	return awssasl.ManagedStreamingIAM(func(ctx context.Context) (awssasl.Auth, error) {
		c, err := creds(ctx)
		if err != nil {
			// The func's error describes the lookup, never key material.
			return awssasl.Auth{}, fmt.Errorf("netsec: MSK IAM credentials: %w", err)
		}
		return awssasl.Auth{
			AccessKey:    c.AccessKey,
			SecretKey:    c.SecretKey,
			SessionToken: c.SessionToken,
			UserAgent:    mskIAMUserAgent,
		}, nil
	}), nil
}
