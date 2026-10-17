package connprobe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/wiring"
)

// kafkaChecks are the ids after config, in order.
var kafkaChecks = []string{"reach", "login", "topics", "offsets", "groups", "acls"}

// errNoAWSInProbe stands in for MSK IAM credentials during validation only;
// the probe never signs in with MSK IAM.
var errNoAWSInProbe = errors.New("connprobe: no AWS credentials in a connection test")

// kafkaSettings is what the config check derives from the settings.
type kafkaSettings struct {
	brokers []string
	proto   string // upper-cased, PLAINTEXT when empty
	mech    string // upper-cased SASL mechanism, "" without SASL
	msk     bool
	tlsCfg  *tls.Config // nil without TLS
	opts    []kgo.Opt   // netsec's options (SASL, and a TLS dialer we replace)
}

func (s kafkaSettings) sasl() bool {
	return s.proto == netsec.SASLPlaintext || s.proto == netsec.SASLSSL
}

// probeKafka checks a Kafka cluster: settings, reach (a metadata request,
// TLS included), SASL login, the pipeline topics, their offsets, the
// pipeline consumer groups, and (when the cluster lets us) whether the
// principal has more ACLs than Piperoom needs. It only reads.
func probeKafka(ctx context.Context, t Target, g *Guard) Result {
	var r Result
	var ks *kafkaSettings
	r.Checks = append(r.Checks, step(func() Check {
		s, bad := kafkaConfig(t.Config)
		if bad != nil {
			return *bad
		}
		ks = s
		login := "no login"
		if s.mech != "" {
			login = "SASL " + s.mech
		}
		return Check{ID: "config", Status: StatusOK, Detail: fmt.Sprintf("%s, %s, %d seed brokers", s.proto, login, len(s.brokers))}
	}))
	if ks == nil {
		skipRest(&r, "settings are incomplete", kafkaChecks...)
		return r
	}
	if ks.msk {
		return probeKafkaMSK(ctx, r, ks, g)
	}

	rec := &dialRecorder{}
	opts := append([]kgo.Opt{}, ks.opts...)
	// netsec's TLS dialer would dial around the guard. DialTLSConfig(nil)
	// clears it (options apply in order) so kgo accepts our Dialer, which
	// dials through the guard and does the same TLS handshake itself.
	opts = append(opts,
		kgo.DialTLSConfig(nil),
		kgo.Dialer(rec.wrap(g, ks.tlsCfg)),
		kgo.SeedBrokers(ks.brokers...),
		kgo.RequestRetries(2),
		kgo.RetryTimeout(DefaultStepTimeout),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		r.Checks[0] = Check{ID: "config", Status: StatusFail, MS: r.Checks[0].MS,
			Problem: "The Kafka client cannot be built from these settings.",
			Fix:     "Check KAFKA_BROKERS, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_* and KAFKA_TLS_*."}
		skipRest(&r, "settings are incomplete", kafkaChecks...)
		return r
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	var login Check
	reach := step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		for _, b := range ks.brokers {
			host, _, _ := net.SplitHostPort(b)
			if _, err := g.Resolve(sctx, host); err != nil {
				return kafkaReachFail(err)
			}
		}
		m, err := adm.BrokerMetadata(sctx)
		if err != nil {
			if lc, ok := saslFail(err); ok {
				login = lc
				return Check{ID: "reach", Status: StatusOK, Detail: "broker answered; login refused"}
			}
			return kafkaReachFail(errors.Join(err, rec.get()))
		}
		if ks.sasl() {
			login = Check{ID: "login", Status: StatusOK, Detail: "SASL " + ks.mech + " accepted"}
		} else {
			login = skipped("login", "no SASL login configured")
		}
		return Check{ID: "reach", Status: StatusOK, Detail: fmt.Sprintf("%d brokers", len(m.Brokers))}
	})
	r.Checks = append(r.Checks, reach)
	if reach.Status == StatusFail {
		skipRest(&r, "no broker reachable", kafkaChecks[1:]...)
		return r
	}
	r.Checks = append(r.Checks, login)
	if login.Status == StatusFail {
		skipRest(&r, "login failed", kafkaChecks[2:]...)
		return r
	}

	principal := kafkaPrincipal(t.Config, ks)
	topics := pipelineTopics(t)
	var existing []string
	if len(topics) == 0 {
		r.Checks = append(r.Checks, skipped("topics", "no pipeline topics to check"))
	} else {
		r.Checks = append(r.Checks, step(func() Check {
			c, ex := kafkaTopicsCheck(ctx, adm, t, topics, principal)
			existing = ex
			return c
		}))
	}
	if len(existing) == 0 {
		r.Checks = append(r.Checks, skipped("offsets", "no pipeline topic to list"))
	} else {
		r.Checks = append(r.Checks, step(func() Check { return kafkaOffsetsCheck(ctx, adm, t, existing, principal) }))
	}
	groups := pipelineGroups(t)
	if len(groups) == 0 {
		r.Checks = append(r.Checks, skipped("groups", "no pipeline consumer groups to check"))
	} else {
		r.Checks = append(r.Checks, step(func() Check { return kafkaGroupsCheck(ctx, adm, groups, principal) }))
	}
	r.Checks = append(r.Checks, step(func() Check { return kafkaACLsCheck(ctx, adm, t, topics, groups, principal) }))
	return r
}

// kafkaConfig validates the settings with netsec's own rules (the server's)
// and explains a refusal in fixed words that name the setting.
func kafkaConfig(cfg config.Config) (*kafkaSettings, *Check) {
	fail := func(problem, fix string) (*kafkaSettings, *Check) {
		return nil, &Check{ID: "config", Status: StatusFail, Problem: problem, Fix: fix}
	}
	brokers := wiring.SplitBrokers(cfg.KafkaBrokers)
	if len(brokers) == 0 {
		return fail("KAFKA_BROKERS is not set.", "Set KAFKA_BROKERS to host:port[,host:port].")
	}
	for _, b := range brokers {
		if h, p, err := net.SplitHostPort(b); err != nil || h == "" || p == "" {
			return fail("A KAFKA_BROKERS entry is not host:port.", "Set KAFKA_BROKERS to host:port[,host:port].")
		}
	}
	s := &kafkaSettings{brokers: brokers, msk: wiring.UsesMSKIAM(cfg)}
	var sec netsec.Kafka
	if s.msk {
		// wiring refuses MSK IAM without the server's AWS config; map the
		// rest as it does and validate with a stand-in credentials func.
		c := cfg
		c.KafkaSASLMechanism = ""
		k, err := wiring.KafkaSecurity(c)
		if err != nil {
			return fail("The Kafka security settings are refused.", "Check KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_* and KAFKA_TLS_*.")
		}
		k.Mechanism = netsec.MechAWSMSKIAM
		k.AWSCredentialsFunc = func(context.Context) (netsec.AWSCredentials, error) {
			return netsec.AWSCredentials{}, errNoAWSInProbe
		}
		sec = k
	} else {
		k, err := wiring.KafkaSecurity(cfg)
		if err != nil {
			return fail("The Kafka security settings are refused.", "Check KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_* and KAFKA_TLS_*.")
		}
		sec = k
	}
	opts, err := sec.Opts()
	if err != nil {
		c := kafkaConfigFail(cfg)
		return nil, &c
	}
	s.opts = opts
	s.proto = strings.ToUpper(strings.TrimSpace(sec.Protocol))
	if s.proto == "" {
		s.proto = netsec.Plaintext
	}
	if s.sasl() {
		s.mech = strings.ToUpper(strings.TrimSpace(sec.Mechanism))
	}
	if s.proto == netsec.SSL || s.proto == netsec.SASLSSL {
		tc, err := sec.TLS.Config()
		if err != nil {
			return fail("The Kafka TLS files cannot be loaded.", "Check KAFKA_TLS_CA_FILE, KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE: readable PEM files, certificate and key set together.")
		}
		s.tlsCfg = tc
	}
	return s, nil
}

// kafkaConfigFail explains why netsec refused the settings, from the settings
// themselves (never from the error text), in netsec's own order.
func kafkaConfigFail(cfg config.Config) Check {
	c := Check{ID: "config", Status: StatusFail}
	proto := strings.ToUpper(strings.TrimSpace(cfg.KafkaSecurityProtocol))
	if proto == "" {
		proto = netsec.Plaintext
	}
	mech := strings.ToUpper(strings.TrimSpace(cfg.KafkaSASLMechanism))
	useTLS := proto == netsec.SSL || proto == netsec.SASLSSL
	useSASL := proto == netsec.SASLPlaintext || proto == netsec.SASLSSL
	tlsSet := cfg.KafkaTLSCAFile != "" || cfg.KafkaTLSCertFile != "" || cfg.KafkaTLSKeyFile != "" || cfg.KafkaTLSServerName != ""
	saslSet := mech != "" || cfg.KafkaSASLUsername != "" || cfg.KafkaSASLPassword != ""
	tlsFix := "Check KAFKA_TLS_CA_FILE, KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE: readable PEM files, certificate and key set together."
	switch {
	case proto != netsec.Plaintext && proto != netsec.SSL && proto != netsec.SASLPlaintext && proto != netsec.SASLSSL:
		c.Problem, c.Fix = "KAFKA_SECURITY_PROTOCOL is not a known protocol.", "Set it to PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL."
	case !useTLS && tlsSet:
		c.Problem, c.Fix = "Kafka TLS settings are set but KAFKA_SECURITY_PROTOCOL has no TLS.", "Use SSL or SASL_SSL, or unset KAFKA_TLS_*."
	case !useSASL && saslSet:
		c.Problem, c.Fix = "A Kafka SASL login is set but KAFKA_SECURITY_PROTOCOL has no SASL.", "Use SASL_SSL, or unset KAFKA_SASL_*."
	case useTLS && tlsLoadFails(cfg):
		c.Problem, c.Fix = "The Kafka TLS files cannot be loaded.", tlsFix
	case mech == netsec.MechAWSMSKIAM && proto != netsec.SASLSSL:
		c.Problem, c.Fix = "AWS_MSK_IAM needs TLS (MSK IAM listeners are TLS only).", "Set KAFKA_SECURITY_PROTOCOL=SASL_SSL."
	case mech == netsec.MechAWSMSKIAM:
		c.Problem, c.Fix = "AWS_MSK_IAM signs in with AWS credentials, but a Kafka user name or password is set.", "Unset KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD."
	case mech == "":
		c.Problem, c.Fix = "KAFKA_SASL_MECHANISM is not set.", "Set it to PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or AWS_MSK_IAM."
	case cfg.KafkaSASLUsername == "" || cfg.KafkaSASLPassword == "":
		c.Problem, c.Fix = "The Kafka SASL login is incomplete.", "Set both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD."
	case mech != netsec.MechPlain && mech != netsec.MechSCRAMSHA256 && mech != netsec.MechSCRAMSHA512:
		c.Problem, c.Fix = "KAFKA_SASL_MECHANISM is not a known mechanism.", "Set it to PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or AWS_MSK_IAM."
	case mech == netsec.MechPlain && !useTLS:
		c.Problem, c.Fix = "SASL PLAIN without TLS would send the password in clear text.", "Set KAFKA_SECURITY_PROTOCOL=SASL_SSL."
	default:
		c.Problem, c.Fix = "The Kafka security settings are refused.", "Check KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_* and KAFKA_TLS_*."
	}
	return c
}

func tlsLoadFails(cfg config.Config) bool {
	_, err := netsec.TLS{CAFile: cfg.KafkaTLSCAFile, CertFile: cfg.KafkaTLSCertFile, KeyFile: cfg.KafkaTLSKeyFile}.Config()
	return err != nil
}

// probeKafkaMSK runs what a probe can check for MSK IAM without the server's
// AWS identity: a guarded TCP and TLS handshake with each seed broker. Login
// and everything after it need a signed SASL session, which the server
// checks at start.
func probeKafkaMSK(ctx context.Context, r Result, ks *kafkaSettings, g *Guard) Result {
	rec := &dialRecorder{}
	dial := rec.wrap(g, ks.tlsCfg)
	reach := step(func() Check {
		sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
		defer cancel()
		for _, b := range ks.brokers {
			conn, err := dial(sctx, "tcp", b)
			if err != nil {
				return kafkaReachFail(err)
			}
			_ = conn.Close() // a probe connection: nothing was sent, nothing to flush
		}
		return Check{ID: "reach", Status: StatusOK, Detail: fmt.Sprintf("TLS handshake with %d of %d seed brokers", len(ks.brokers), len(ks.brokers))}
	})
	r.Checks = append(r.Checks, reach)
	if reach.Status == StatusFail {
		skipRest(&r, "no broker reachable", kafkaChecks[1:]...)
		return r
	}
	skipRest(&r, "AWS_MSK_IAM signs in with the server's AWS identity, which a connection test does not load; the server checks the login at start", kafkaChecks[1:]...)
	return r
}

// dialRecorder dials through the guard (then TLS, if set) and keeps the last
// dial error: kgo retries dials and may report only its own timeout, while
// the reason (a guard refusal, a bad certificate) is in the dial error.
type dialRecorder struct {
	mu   sync.Mutex
	last error
}

func (d *dialRecorder) get() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.last
}

func (d *dialRecorder) wrap(g *Guard, tlsCfg *tls.Config) func(ctx context.Context, network, host string) (net.Conn, error) {
	return func(ctx context.Context, network, host string) (net.Conn, error) {
		conn, err := g.DialContext(ctx, network, host)
		if err == nil && tlsCfg != nil {
			conn, err = tlsHandshake(ctx, conn, tlsCfg, host)
		}
		if err != nil {
			d.mu.Lock()
			d.last = err
			d.mu.Unlock()
			return nil, err
		}
		return conn, nil
	}
}

// tlsHandshake does what kgo's DialTLSConfig dialer does: clone the config,
// default ServerName to the broker host, and handshake.
func tlsHandshake(ctx context.Context, conn net.Conn, cfg *tls.Config, host string) (net.Conn, error) {
	c := cfg.Clone()
	if c.ServerName == "" {
		server, _, err := net.SplitHostPort(host)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("connprobe: split %q: %w", host, err)
		}
		c.ServerName = server
	}
	tc := tls.Client(conn, c)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connprobe: tls handshake: %w", err)
	}
	return tc, nil
}

// saslFail turns a SASL error into the failed login check.
func saslFail(err error) (Check, bool) {
	c := Check{ID: "login", Status: StatusFail}
	switch {
	case errors.Is(err, kerr.SaslAuthenticationFailed):
		c.Problem = "The broker refused the SASL user name or password."
		c.Fix = "Check KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD and KAFKA_SASL_MECHANISM."
	case errors.Is(err, kerr.UnsupportedSaslMechanism):
		c.Problem = "The broker does not offer the configured SASL mechanism."
		c.Fix = "Set KAFKA_SASL_MECHANISM to a mechanism the listener enables."
	case errors.Is(err, kerr.IllegalSaslState):
		c.Problem = "The broker refused the SASL exchange."
		c.Fix = "Check that KAFKA_SECURITY_PROTOCOL and KAFKA_SASL_MECHANISM match the listener."
	default:
		return Check{}, false
	}
	return c, true
}

// kafkaReachFail turns a metadata or dial error into the failed reach check.
func kafkaReachFail(err error) Check {
	c := Check{ID: "reach", Status: StatusFail}
	var cve *tls.CertificateVerificationError
	var uae x509.UnknownAuthorityError
	var hne x509.HostnameError
	var cie x509.CertificateInvalidError
	var rhe tls.RecordHeaderError
	var dnse *net.DNSError
	listenerFix := "Check KAFKA_BROKERS (host and port), the network, and the brokers' advertised listeners."
	switch {
	case errors.Is(err, ErrBlockedAddress):
		c.Problem = "A broker address is not allowed: it is a link-local, metadata or otherwise blocked address."
		c.Fix = "Check KAFKA_BROKERS and the brokers' advertised listeners."
	case errors.As(err, &cve), errors.As(err, &uae), errors.As(err, &hne), errors.As(err, &cie):
		c.Problem = "The broker TLS certificate is not trusted or does not match the host."
		c.Fix = "Set KAFKA_TLS_CA_FILE to the CA that signed it, or KAFKA_TLS_SERVER_NAME to the name it carries."
	case errors.As(err, &rhe):
		c.Problem = "The broker did not answer TLS."
		c.Fix = "Use PLAINTEXT or SASL_PLAINTEXT in KAFKA_SECURITY_PROTOCOL if the listener has no TLS, or check the port."
	case errors.As(err, &dnse):
		c.Problem = "A broker host name does not resolve."
		c.Fix = listenerFix
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET):
		c.Problem = "The broker closed the connection before answering."
		c.Fix = "Check that KAFKA_SECURITY_PROTOCOL and KAFKA_SASL_MECHANISM match the listener (it may need TLS or SASL); some brokers also drop a wrong KAFKA_SASL_USERNAME or KAFKA_SASL_PASSWORD this way."
	case isTimeout(err):
		c.Problem = "No answer from any broker within 10 s."
		c.Fix = listenerFix
	default:
		c.Problem = "Cannot connect to any broker."
		c.Fix = listenerFix
	}
	return c
}

// principalInfo is the Kafka principal the probe connects as. Fix texts name
// it by label, never by the user name itself, which the admin API treats as
// secret.
type principalInfo struct {
	// filter is the principal to match ACLs on, "" when it cannot be known.
	filter string
	// label is how a fix names it.
	label string
}

func kafkaPrincipal(cfg config.Config, ks *kafkaSettings) principalInfo {
	switch {
	case ks.sasl():
		return principalInfo{filter: "User:" + cfg.KafkaSASLUsername, label: "User:<KAFKA_SASL_USERNAME>"}
	case cfg.KafkaTLSCertFile != "":
		// The principal is the client certificate's name, after the
		// brokers' ssl.principal.mapping.rules, which we cannot see.
		return principalInfo{label: "<principal of the KAFKA_TLS_CERT_FILE certificate>"}
	default:
		return principalInfo{filter: "User:ANONYMOUS", label: "User:ANONYMOUS"}
	}
}

// topicPrefixOf returns the pipeline prefix a topic was built from.
func topicPrefixOf(t Target, topic string) string {
	for _, p := range t.Pipelines {
		if p.TopicPrefix != "" && strings.HasPrefix(topic, p.TopicPrefix+".") {
			return p.TopicPrefix
		}
	}
	return topic
}

func topicACLFix(prefixes []string, principal principalInfo) string {
	var cmds []string
	for _, p := range prefixes {
		cmds = append(cmds, "kafka-acls --bootstrap-server <bootstrap> --add --allow-principal "+principal.label+
			" --operation Describe --operation Read --operation DescribeConfigs --topic "+p+". --resource-pattern-type prefixed")
	}
	return strings.Join(cmds, " ; ")
}

// kafkaTopicsCheck describes each pipeline topic and returns the ones that
// exist and are describable.
func kafkaTopicsCheck(ctx context.Context, adm *kadm.Client, t Target, topics []string, principal principalInfo) (Check, []string) {
	sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
	defer cancel()
	var existing, missing, denied, details, problems []string
	deniedPrefixes := map[string]bool{}
	for _, tp := range topics {
		m, err := adm.Metadata(sctx, tp)
		if err != nil {
			if errors.Is(err, kerr.TopicAuthorizationFailed) {
				denied = append(denied, tp)
				deniedPrefixes[topicPrefixOf(t, tp)] = true
				continue
			}
			problems = append(problems, "describing topic "+tp+" failed ("+kafkaErrWord(err)+")")
			continue
		}
		td, ok := m.Topics[tp]
		switch {
		case !ok || errors.Is(td.Err, kerr.UnknownTopicOrPartition):
			missing = append(missing, tp)
		case errors.Is(td.Err, kerr.TopicAuthorizationFailed):
			denied = append(denied, tp)
			deniedPrefixes[topicPrefixOf(t, tp)] = true
		case td.Err != nil:
			problems = append(problems, "topic "+tp+" reports an error ("+kafkaErrWord(td.Err)+")")
		default:
			existing = append(existing, tp)
			details = append(details, fmt.Sprintf("%s (%d partitions)", tp, len(td.Partitions)))
		}
	}
	c := Check{ID: "topics", Status: StatusOK, Detail: strings.Join(details, "; ")}
	var parts []string
	if len(missing) > 0 {
		parts = append(parts, "topic not found: "+strings.Join(missing, ", "))
	}
	if len(denied) > 0 {
		parts = append(parts, "the principal may not describe topic: "+strings.Join(denied, ", "))
	}
	parts = append(parts, problems...)
	if len(parts) > 0 {
		c.Status = StatusFail
		c.Problem = upperFirst(strings.Join(parts, "; ")) + "."
		switch {
		case len(denied) > 0:
			c.Fix = topicACLFix(sortedKeys(deniedPrefixes), principal)
		case len(missing) > 0:
			c.Fix = "Check topic_prefix and source_tables of the pipeline in owners.yaml, and that the connector has written its first change."
		default:
			c.Fix = "Check the brokers' health (offline partitions, leader elections)."
		}
	}
	return c, existing
}

// kafkaOffsetsCheck lists start and end offsets of the existing topics.
func kafkaOffsetsCheck(ctx context.Context, adm *kadm.Client, t Target, topics []string, principal principalInfo) Check {
	sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
	defer cancel()
	fail := func(err error) Check {
		c := Check{ID: "offsets", Status: StatusFail, Problem: "Listing the offsets of the pipeline topics failed (" + kafkaErrWord(err) + ")."}
		if errors.Is(err, kerr.TopicAuthorizationFailed) {
			prefixes := map[string]bool{}
			for _, tp := range topics {
				prefixes[topicPrefixOf(t, tp)] = true
			}
			c.Fix = topicACLFix(sortedKeys(prefixes), principal)
		} else {
			c.Fix = "Check the brokers' health (offline partitions, leader elections)."
		}
		return c
	}
	start, err := adm.ListStartOffsets(sctx, topics...)
	if err == nil {
		err = start.Error()
	}
	if err != nil {
		return fail(err)
	}
	end, err := adm.ListEndOffsets(sctx, topics...)
	if err == nil {
		err = end.Error()
	}
	if err != nil {
		return fail(err)
	}
	n := 0
	end.Each(func(kadm.ListedOffset) { n++ })
	return Check{ID: "offsets", Status: StatusOK, Detail: fmt.Sprintf("start and end offsets for %d partitions of %d topics", n, len(topics))}
}

// groupStates are the states Kafka reports; anything else shows as unknown.
var groupStates = map[string]bool{
	"Stable": true, "Empty": true, "PreparingRebalance": true, "CompletingRebalance": true,
	"Dead": true, "Assigning": true, "Reconciling": true,
}

// kafkaGroupsCheck describes each pipeline consumer group.
func kafkaGroupsCheck(ctx context.Context, adm *kadm.Client, groups []string, principal principalInfo) Check {
	sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
	defer cancel()
	var details, inactive, denied, problems []string
	for _, grp := range groups {
		ds, err := adm.DescribeGroups(sctx, grp)
		dg, ok := ds[grp]
		if !ok {
			switch {
			case err == nil, errors.Is(err, kerr.GroupIDNotFound):
				inactive = append(inactive, grp)
			case errors.Is(err, kerr.GroupAuthorizationFailed):
				denied = append(denied, grp)
			default:
				problems = append(problems, "describing group "+grp+" failed ("+kafkaErrWord(err)+")")
			}
			continue
		}
		switch {
		case errors.Is(dg.Err, kerr.GroupIDNotFound):
			inactive = append(inactive, grp)
		case errors.Is(dg.Err, kerr.GroupAuthorizationFailed):
			denied = append(denied, grp)
		case dg.Err != nil:
			problems = append(problems, "describing group "+grp+" failed ("+kafkaErrWord(dg.Err)+")")
		case dg.State == "Dead" || dg.State == "":
			inactive = append(inactive, grp)
		default:
			state := dg.State
			if !groupStates[state] {
				state = "unknown"
			}
			details = append(details, fmt.Sprintf("%s %s, %d members", grp, state, len(dg.Members)))
		}
	}
	c := Check{ID: "groups", Status: StatusOK, Detail: strings.Join(details, "; ")}
	switch {
	case len(denied) > 0 || len(problems) > 0:
		var parts []string
		if len(denied) > 0 {
			parts = append(parts, "the principal may not describe group: "+strings.Join(denied, ", "))
		}
		parts = append(parts, problems...)
		c.Status = StatusFail
		c.Problem = upperFirst(strings.Join(parts, "; ")) + "."
		var cmds []string
		for _, grp := range denied {
			cmds = append(cmds, "kafka-acls --bootstrap-server <bootstrap> --add --allow-principal "+principal.label+" --operation Describe --group "+grp)
		}
		if len(cmds) > 0 {
			c.Fix = strings.Join(cmds, " ; ")
		} else {
			c.Fix = "Check the brokers' health and the group coordinator."
		}
	case len(inactive) > 0:
		c.Status = StatusWarn
		c.Problem = "Consumer group not active yet: " + strings.Join(inactive, ", ") + "."
		c.Fix = "Start the consumer, or check consumer_groups of the pipeline in owners.yaml."
	}
	return c
}

// tooMuch are the rights Piperoom must not hold, per resource type
// (docs/pilot/permissions.md, Kafka): no writes or alters on topics, no Read
// on groups (it would allow committing offsets).
var tooMuch = map[kmsg.ACLResourceType]map[kadm.ACLOperation]string{
	kmsg.ACLResourceTypeTopic: {
		kadm.OpAll: "All", kadm.OpWrite: "Write", kadm.OpCreate: "Create", kadm.OpDelete: "Delete",
		kadm.OpAlter: "Alter", kadm.OpAlterConfigs: "AlterConfigs",
	},
	kmsg.ACLResourceTypeGroup: {
		kadm.OpAll: "All", kadm.OpRead: "Read", kadm.OpDelete: "Delete",
	},
}

// kafkaACLsCheck describes the ACLs of the principal on the pipeline topics
// and groups. Describing ACLs needs Describe on Cluster, which Piperoom does
// not ask for, so a refusal is a skip, not a failure.
func kafkaACLsCheck(ctx context.Context, adm *kadm.Client, t Target, topics, groups []string, principal principalInfo) Check {
	if principal.filter == "" {
		return skipped("acls", "the principal comes from the client certificate; compare its ACLs with docs/pilot/permissions.md by hand")
	}
	prefixes := pipelinePrefixes(t)
	resTopics := uniqueSorted(append(append([]string{}, topics...), dotted(prefixes)...))
	if len(resTopics) == 0 && len(groups) == 0 {
		return skipped("acls", "no pipeline topics or groups to check")
	}
	b := kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternMatch).
		Operations(kadm.OpAny).
		Allow(principal.filter, "User:*").AllowHosts()
	if len(resTopics) > 0 {
		b.Topics(resTopics...)
	}
	if len(groups) > 0 {
		b.Groups(groups...)
	}
	if err := b.ValidateDescribe(); err != nil {
		return skipped("acls", "the ACL filter could not be built")
	}
	sctx, cancel := context.WithTimeout(ctx, DefaultStepTimeout)
	defer cancel()
	results, err := adm.DescribeACLs(sctx, b)
	if err == nil {
		for _, res := range results {
			if res.Err != nil {
				err = res.Err
				break
			}
		}
	}
	switch {
	case errors.Is(err, kerr.ClusterAuthorizationFailed):
		return skipped("acls", "the principal may not describe ACLs (needs Describe on Cluster, which Piperoom does not ask for); compare them with docs/pilot/permissions.md by hand")
	case errors.Is(err, kerr.SecurityDisabled):
		return skipped("acls", "the cluster has no ACL authorizer, so ACLs do not limit this principal")
	case err != nil:
		return skipped("acls", "ACLs could not be described ("+kafkaErrWord(err)+")")
	}

	type excess struct{ op, resType, res, pattern, principal string }
	seen := map[excess]bool{}
	var found []excess
	total := 0
	for _, res := range results {
		for _, d := range res.Described {
			if d.Permission != kmsg.ACLPermissionTypeAllow || (d.Principal != principal.filter && d.Principal != "User:*") {
				continue
			}
			if !aclCovers(d, topics, prefixes, groups) {
				continue
			}
			total++
			name, bad := tooMuch[d.Type][d.Operation]
			if !bad {
				continue
			}
			e := excess{op: name, res: d.Name, principal: principal.label}
			if d.Principal == "User:*" {
				e.principal = "User:*"
			}
			if d.Type == kmsg.ACLResourceTypeTopic {
				e.resType = "topic"
			} else {
				e.resType = "group"
			}
			if d.Pattern == kadm.ACLPatternPrefixed {
				e.pattern = "prefixed"
			} else {
				e.pattern = "literal"
			}
			if !seen[e] {
				seen[e] = true
				found = append(found, e)
			}
		}
	}
	if len(found) == 0 {
		return Check{ID: "acls", Status: StatusOK, Detail: fmt.Sprintf("%d ACLs on pipeline topics and groups, none more than needed", total)}
	}
	sort.Slice(found, func(i, j int) bool {
		a, b := found[i], found[j]
		return a.resType+a.res+a.op+a.principal < b.resType+b.res+b.op+b.principal
	})
	var what, cmds []string
	for _, e := range found {
		what = append(what, fmt.Sprintf("%s on %s %s (%s)", e.op, e.resType, e.res, e.pattern))
		cmds = append(cmds, fmt.Sprintf("kafka-acls --bootstrap-server <bootstrap> --remove --allow-principal %s --operation %s --%s %s --resource-pattern-type %s",
			e.principal, e.op, e.resType, shellWord(e.res), e.pattern))
	}
	return Check{ID: "acls", Status: StatusWarn,
		Problem: "The principal has more rights than Piperoom needs: " + strings.Join(what, ", ") + ".",
		Fix:     strings.Join(cmds, " ; ")}
}

// aclCovers reports whether a described ACL applies to a pipeline topic,
// topic prefix, or group (the broker's MATCH filter may return wider ones).
func aclCovers(d kadm.DescribedACL, topics, prefixes, groups []string) bool {
	var names []string
	switch d.Type {
	case kmsg.ACLResourceTypeTopic:
		names = append(append(names, topics...), dotted(prefixes)...)
	case kmsg.ACLResourceTypeGroup:
		names = groups
	default:
		return false
	}
	for _, n := range names {
		switch {
		case d.Name == "*",
			d.Pattern == kadm.ACLPatternLiteral && d.Name == n,
			d.Pattern == kadm.ACLPatternPrefixed && strings.HasPrefix(n, d.Name):
			return true
		}
	}
	return false
}

// dotted returns "<prefix>." for each prefix, the start of every topic name.
func dotted(prefixes []string) []string {
	out := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		out = append(out, p+".")
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// shellWord quotes "*" so a copied command does not glob.
func shellWord(s string) string {
	if s == "*" {
		return "'*'"
	}
	return s
}

// kafkaErrWord names a Kafka error by its protocol code name, or a fixed
// word; never the broker's message text.
func kafkaErrWord(err error) string {
	var ke *kerr.Error
	switch {
	case errors.As(err, &ke):
		return ke.Message
	case isTimeout(err):
		return "timed out after 10 s"
	default:
		return "network error"
	}
}
