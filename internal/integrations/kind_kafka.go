package integrations

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/integrations/spec"
)

// Kafka kind ids.
const (
	KindKafka               = "kafka"
	KindKafkaConfluentCloud = "kafka-confluent-cloud"
)

const brokersPattern = `^\s*[A-Za-z0-9._\[\]:-]+(\s*,\s*[A-Za-z0-9._\[\]:-]+)*\s*$`

// kafkaProbe is what the Kafka checks ask a cluster.
type kafkaProbe interface {
	// Brokers returns how many brokers the metadata response lists.
	Brokers(ctx context.Context) (int, error)
	// Groups returns how many consumer groups the principal can see.
	Groups(ctx context.Context) (int, error)
	// WriteACLs describes the ACLs that let principal write ("Topic orders
	// WRITE"). Any error means ACLs cannot be read here.
	WriteACLs(ctx context.Context, principal string) ([]string, error)
	Close()
}

type kafkaOpener func(brokers []string, opts []kgo.Opt) (kafkaProbe, error)

// kafkaTarget is a parsed Kafka form.
type kafkaTarget struct {
	Brokers   []string
	Protocol  string
	Mechanism string
	Username  string
	Password  netsec.Secret
	TLS       tlsMaterial
	Confluent bool
}

func (t kafkaTarget) useTLS() bool { return t.Protocol == netsec.SSL || t.Protocol == netsec.SASLSSL }
func (t kafkaTarget) useSASL() bool {
	return t.Protocol == netsec.SASLPlaintext || t.Protocol == netsec.SASLSSL
}

func parseKafka(v spec.Values, confluent bool) (kafkaTarget, error) {
	t := kafkaTarget{Confluent: confluent}
	if confluent {
		t.Brokers = splitList(v.Get("bootstrap"))
		t.Protocol, t.Mechanism = netsec.SASLSSL, netsec.MechPlain
		t.Username, t.Password = strings.TrimSpace(v.Get("api_key")), v.Secret("api_secret")
		if len(t.Brokers) == 0 {
			return t, &spec.ValidationError{Field: "bootstrap", Message: "required"}
		}
	} else {
		t.Brokers = splitList(v.Get("brokers"))
		t.Protocol = strings.ToUpper(strings.TrimSpace(v.Get("protocol")))
		if t.Protocol == "" {
			t.Protocol = netsec.SASLSSL
		}
		t.Mechanism = strings.ToUpper(strings.TrimSpace(v.Get("mechanism")))
		t.Username, t.Password = strings.TrimSpace(v.Get("username")), v.Secret("password")
		t.TLS = tlsMaterial{CA: v.Secret("ca_cert"), Cert: v.Secret("client_cert"), Key: v.Secret("client_key"), ServerName: strings.TrimSpace(v.Get("server_name"))}
		if len(t.Brokers) == 0 {
			return t, &spec.ValidationError{Field: "brokers", Message: "required"}
		}
	}
	for _, b := range t.Brokers {
		if _, _, err := net.SplitHostPort(b); err != nil {
			return t, &spec.ValidationError{Field: brokersField(confluent), Message: "each broker must be host:port"}
		}
	}
	if !t.useSASL() {
		t.Mechanism = ""
		if t.Username != "" || t.Password != "" {
			return t, &spec.ValidationError{Field: "protocol", Message: "a SASL login is set but the protocol has no SASL"}
		}
	}
	if !t.useTLS() && !t.TLS.isZero() {
		return t, &spec.ValidationError{Field: "protocol", Message: "TLS certificates are set but the protocol has no TLS"}
	}
	if t.Mechanism == netsec.MechAWSMSKIAM {
		return t, &spec.ValidationError{Field: "mechanism", Message: "AWS MSK IAM uses the AWS role card, not this form"}
	}
	// netsec holds the clear-text rules (no PLAIN without TLS, login pairs).
	if _, err := (netsec.Kafka{Protocol: t.Protocol, Mechanism: t.Mechanism, Username: t.Username, Password: t.Password}).Opts(); err != nil {
		return t, &spec.ValidationError{Field: "protocol", Message: truncate(strings.TrimPrefix(errText(err), "netsec: invalid configuration: "), maxDetail)}
	}
	return t, nil
}

func brokersField(confluent bool) string {
	if confluent {
		return "bootstrap"
	}
	return "brokers"
}

// opts builds the kgo connection options. TLS is done in the dialer, after
// the guard, because kgo refuses Dialer together with DialTLSConfig.
func (t kafkaTarget) opts(dial dialFunc) ([]kgo.Opt, error) {
	var tlsCfg *tls.Config
	if t.useTLS() {
		c, err := t.TLS.config()
		if err != nil {
			return nil, err
		}
		tlsCfg = c
	}
	opts := []kgo.Opt{kgo.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dial(ctx, network, addr)
		if err != nil || tlsCfg == nil {
			return c, err
		}
		cfg := tlsCfg.Clone()
		if cfg.ServerName == "" {
			host, _, _ := net.SplitHostPort(addr)
			cfg.ServerName = host
		}
		tc := tls.Client(c, cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = c.Close()
			return nil, err
		}
		return tc, nil
	})}
	if t.useSASL() {
		var m sasl.Mechanism
		switch t.Mechanism {
		case netsec.MechPlain:
			m = plain.Auth{User: t.Username, Pass: t.Password.Reveal()}.AsMechanism()
		case netsec.MechSCRAMSHA256:
			m = scram.Auth{User: t.Username, Pass: t.Password.Reveal()}.AsSha256Mechanism()
		case netsec.MechSCRAMSHA512:
			m = scram.Auth{User: t.Username, Pass: t.Password.Reveal()}.AsSha512Mechanism()
		default:
			return nil, fmt.Errorf("%w: SASL mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", errBadInput)
		}
		opts = append(opts, kgo.SASL(m))
	}
	return opts, nil
}

type kadmProbe struct {
	cl  *kgo.Client
	adm *kadm.Client
}

func realKafkaOpen(brokers []string, opts []kgo.Opt) (kafkaProbe, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("integrations: kafka client: %w", err)
	}
	return &kadmProbe{cl: cl, adm: kadm.NewClient(cl)}, nil
}

func (p *kadmProbe) Brokers(ctx context.Context) (int, error) {
	m, err := p.adm.BrokerMetadata(ctx)
	if err != nil {
		return 0, err
	}
	return len(m.Brokers), nil
}

// Groups lists the groups and then describes one of them (§4.2 names
// DescribeGroups): the agent reads lag per group, which needs DESCRIBE on the
// group, and listing alone does not prove that.
func (p *kadmProbe) Groups(ctx context.Context) (int, error) {
	g, err := p.adm.ListGroups(ctx)
	if err != nil {
		return 0, err
	}
	names := g.Groups()
	if len(names) == 0 {
		return 0, nil
	}
	d, err := p.adm.DescribeGroups(ctx, names[0])
	if err != nil {
		return 0, err
	}
	if err := d.Error(); err != nil {
		return 0, err
	}
	return len(names), nil
}

// WriteACLs reports ALLOW entries only. A DENY that overrides an allow is not
// taken into account, so the check can warn about a principal that in fact
// cannot produce; that errs on the safe side for a warning.
func (p *kadmProbe) WriteACLs(ctx context.Context, principal string) ([]string, error) {
	b := kadm.NewACLs().Allow(principal, "User:*").AllowHosts().AnyResource().
		Operations(kadm.OpWrite, kadm.OpAll).ResourcePatternType(kadm.ACLPatternAny)
	res, err := p.adm.DescribeACLs(ctx, b)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range res {
		if r.Err != nil {
			return nil, r.Err
		}
		for _, d := range r.Described {
			if d.Permission == kmsg.ACLPermissionTypeAllow {
				out = append(out, fmt.Sprintf("%s %s %s", d.Type, d.Name, d.Operation))
			}
		}
	}
	return out, nil
}

func (p *kadmProbe) Close() { p.cl.Close() }

func (d Deps) kafkaTest(confluent bool) func(ctx context.Context, v spec.Values) []spec.Check {
	return func(ctx context.Context, v spec.Values) []spec.Check {
		ctx, cancel := testCtx(ctx, d.TestTimeout)
		defer cancel()
		cl := newChecklist(ctx, d.CheckTimeout, v)
		t, err := parseKafka(v, confluent)
		if err != nil {
			cl.add("input", "Form values are valid", fail(err.Error(), ""))
			return cl.result()
		}
		host, port, _ := net.SplitHostPort(t.Brokers[0])
		if cl.run("reach", "Can reach "+t.Brokers[0], reachCheck(d.dial(), host, port)) != spec.CheckPass {
			cl.skip("login", "Login works", "skipped: the broker is not reachable")
			return cl.result()
		}
		if t.useTLS() {
			st := cl.run("tls", "TLS handshake", func(ctx context.Context) outcome {
				cfg, err := t.TLS.config()
				if err != nil {
					return fail(errText(err), "Paste the CA certificate as PEM text.")
				}
				if cfg.ServerName == "" {
					cfg.ServerName = host
				}
				c, err := d.dial()(ctx, "tcp", t.Brokers[0])
				if err != nil {
					return fail("cannot connect: "+errText(err), "")
				}
				defer c.Close()
				if err := tls.Client(c, cfg).HandshakeContext(ctx); err != nil {
					return fail("TLS handshake failed: "+errText(err),
						"Paste the CA that signs the broker certificate, or set the TLS server name to the name on the certificate.")
				}
				return pass("")
			})
			if st != spec.CheckPass {
				cl.skip("login", "Login works", "skipped: the TLS handshake failed")
				return cl.result()
			}
		}
		opts, err := t.opts(d.dial())
		if err != nil {
			cl.add("login", "Login works", fail(errText(err), ""))
			return cl.result()
		}
		probe, err := d.kafkaOpen(t.Brokers, opts)
		if err != nil {
			cl.add("login", "Login works", fail(errText(err), ""))
			return cl.result()
		}
		defer probe.Close()

		st := cl.run("metadata", "Login works and the cluster lists its brokers", func(ctx context.Context) outcome {
			n, err := probe.Brokers(ctx)
			switch {
			case errors.Is(err, kerr.SaslAuthenticationFailed):
				return fail("SASL authentication failed", kafkaLoginFix(t))
			case errors.Is(err, kerr.ClusterAuthorizationFailed):
				return fail("the principal may not describe the cluster", "Grant DESCRIBE on the cluster to "+kafkaPrincipal(t)+".")
			case err != nil && isTLSError(err):
				return fail("TLS failed: "+errText(err), "Paste the CA that signs the broker certificate.")
			case err != nil:
				return fail("metadata request failed: "+errText(err), kafkaLoginFix(t))
			case n == 0:
				return fail("the metadata response lists no brokers", "Check the bootstrap address; it must be a broker of this cluster.")
			}
			return pass(fmt.Sprintf("%d broker(s)", n))
		})
		if st != spec.CheckPass {
			cl.skip("groups", "Can list consumer groups", "skipped: the login failed")
			return cl.result()
		}
		cl.run("groups", "Can list consumer groups", func(ctx context.Context) outcome {
			n, err := probe.Groups(ctx)
			if err != nil {
				if errors.Is(err, kerr.GroupAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) {
					return fail("the principal may not list or describe consumer groups", groupACLFix(t))
				}
				return fail("listing groups failed: "+errText(err), groupACLFix(t))
			}
			return pass(fmt.Sprintf("%d group(s) visible", n))
		})
		switch {
		case t.Confluent:
			cl.skip("read_only", "Principal cannot produce", "skipped: Confluent Cloud ACLs belong to the API key's service account; check them in the Confluent console (Accounts & access → ACLs). Use a key with only DESCRIBE and READ.")
		case t.Username == "":
			cl.skip("read_only", "Principal cannot produce", "skipped: no SASL principal to look up ACLs for")
		default:
			cl.run("read_only", "Principal cannot produce", func(ctx context.Context) outcome {
				acls, err := probe.WriteACLs(ctx, kafkaPrincipal(t))
				if err != nil {
					return skippedOutcome("skipped: the cluster does not let this principal read ACLs (" + errText(err) + ")")
				}
				if len(acls) > 0 {
					return warn(fmt.Sprintf("%s may write: %s", kafkaPrincipal(t), strings.Join(acls, "; ")),
						fmt.Sprintf("Remove each ACL above with its own resource and operation, e.g. kafka-acls --bootstrap-server <broker> --remove --allow-principal %s --operation Write --topic <name>. (DENY ACLs are not counted.)", kafkaPrincipal(t)))
				}
				return pass("")
			})
		}
		return cl.result()
	}
}

func kafkaPrincipal(t kafkaTarget) string {
	if t.Username == "" {
		return "the principal"
	}
	return "User:" + t.Username
}

func kafkaLoginFix(t kafkaTarget) string {
	if t.Confluent {
		return "Check the API key and secret: they must be a Kafka cluster key (not a Cloud or Schema Registry key)."
	}
	return "Check the username, password and SASL mechanism (SCRAM users are per mechanism)."
}

func groupACLFix(t kafkaTarget) string {
	if t.Confluent {
		return "Add a DESCRIBE ACL on consumer groups (prefix *) to the API key's service account."
	}
	return fmt.Sprintf("kafka-acls --bootstrap-server <broker> --add --allow-principal %s --operation Describe --group '*'", kafkaPrincipal(t))
}

// errNoRegistry answers every call of the stub registry.
var errNoRegistry = errors.New("integrations: Schema Registry is not connected")

type failRT struct{}

func (failRT) RoundTrip(*http.Request) (*http.Response, error) { return nil, errNoRegistry }

func (d Deps) kafkaBuild(confluent bool) func(ctx context.Context, v spec.Values) (any, func(), error) {
	return func(_ context.Context, v spec.Values) (any, func(), error) {
		t, err := parseKafka(v, confluent)
		if err != nil {
			return nil, nil, err
		}
		opts, err := t.opts(d.dial())
		if err != nil {
			return nil, nil, err
		}
		var sr *schemareg.Client
		if d.KafkaRegistry != nil {
			sr = d.KafkaRegistry()
		}
		if sr == nil {
			// kafka.Client dereferences its registry for Avro keys; a stub
			// turns that into an error instead of a nil-pointer panic.
			sr = schemareg.New("http://schema-registry.not-connected.invalid", &http.Client{Transport: failRT{}})
		}
		c, err := kafka.New(t.Brokers, sr, opts...)
		if err != nil {
			return nil, nil, scrubErr(fmt.Errorf("integrations: kafka: %w", err), t.Password)
		}
		return c, c.Close, nil
	}
}

func (d Deps) kafkaKinds() []spec.Kind {
	reads := []string{"topic names, partition offsets and configs", "consumer group lag", "single records by key during an investigation (sizes and keys, not shown in full)"}
	never := []string{"produces to any topic", "commits consumer offsets for your groups"}
	return []spec.Kind{
		{
			ID: KindKafka, Category: spec.CategoryKafka, Title: "Apache Kafka",
			Summary: "Self-hosted, Strimzi or any Kafka-compatible cluster", Logo: "kafka",
			Reads: reads, Never: never, Required: true,
			Fields: []spec.Field{
				{Name: "brokers", Label: "Bootstrap brokers", Help: "Comma list of host:port", Type: spec.FieldText, Required: true, Pattern: brokersPattern},
				{Name: "protocol", Label: "Security protocol", Type: spec.FieldSelect, Default: netsec.SASLSSL, Options: []string{netsec.SASLSSL, netsec.SSL, netsec.SASLPlaintext, netsec.Plaintext}},
				{Name: "mechanism", Label: "SASL mechanism", Type: spec.FieldSelect, Default: netsec.MechSCRAMSHA512, Options: []string{netsec.MechSCRAMSHA512, netsec.MechSCRAMSHA256, netsec.MechPlain}},
				{Name: "username", Label: "SASL username", Type: spec.FieldText},
				{Name: "password", Label: "SASL password", Type: spec.FieldSecret},
				{Name: "ca_cert", Label: "CA certificate", Help: "PEM; empty = the system's trusted CAs", Type: spec.FieldPEM},
				{Name: "client_cert", Label: "Client certificate", Help: "PEM, for mutual TLS", Type: spec.FieldPEM},
				{Name: "client_key", Label: "Client key", Help: "PEM, for mutual TLS", Type: spec.FieldPEM},
				{Name: "server_name", Label: "TLS server name", Help: "Empty = the broker host", Type: spec.FieldText, Pattern: `^[A-Za-z0-9.-]*$`},
			},
			SetupScript: kafkaSetupScript,
			Test:        d.kafkaTest(false), Build: d.kafkaBuild(false),
		},
		{
			ID: KindKafkaConfluentCloud, Category: spec.CategoryKafka, Title: "Confluent Cloud",
			Summary: "Kafka on Confluent Cloud (SASL_SSL with an API key)", Logo: "confluent",
			Reads: reads, Never: never, Required: true,
			Fields: []spec.Field{
				{Name: "bootstrap", Label: "Bootstrap server", Help: "pkc-xxxxx.<region>.<cloud>.confluent.cloud:9092", Type: spec.FieldText, Required: true, Pattern: brokersPattern},
				{Name: "api_key", Label: "API key", Type: spec.FieldText, Required: true, Pattern: `^[A-Za-z0-9]+$`},
				{Name: "api_secret", Label: "API secret", Type: spec.FieldSecret, Required: true},
			},
			Test: d.kafkaTest(true), Build: d.kafkaBuild(true),
		},
	}
}

// kafkaSetupScript prints read-only ACLs for the form's user.
func kafkaSetupScript(v spec.Values) string {
	user := strings.TrimSpace(v.Get("username"))
	if !regexpSafeUser(user) {
		user = "piperoom"
	}
	p := "User:" + user
	var b strings.Builder
	b.WriteString("# Read-only Kafka ACLs for Piperoom (Apache Kafka kafka-acls). Replace <broker>.\n")
	fmt.Fprintf(&b, "kafka-acls --bootstrap-server <broker> --add --allow-principal %s --operation Describe --cluster\n", p)
	fmt.Fprintf(&b, "kafka-acls --bootstrap-server <broker> --add --allow-principal %s --operation Describe --operation DescribeConfigs --operation Read --topic '*'\n", p)
	fmt.Fprintf(&b, "kafka-acls --bootstrap-server <broker> --add --allow-principal %s --operation Describe --group '*'\n", p)
	b.WriteString("# Do not grant Write, Create, Delete or Alter.\n")
	return b.String()
}

func regexpSafeUser(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' || r == '@') {
			return false
		}
	}
	return true
}
