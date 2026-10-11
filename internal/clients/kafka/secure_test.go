package kafka

import (
	"crypto/tls"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"piperoom/internal/clients/netsec"
)

// TestNewSecureOptsReachEveryClient checks the SASL/TLS options are on the
// long-lived admin client and on the short-lived consumers built by newKgo
// (FindRecord, TopicOffsets), which would otherwise connect without login.
func TestNewSecureOptsReachEveryClient(t *testing.T) {
	c, err := NewSecure([]string{"127.0.0.1:1"}, nil, netsec.Kafka{
		Protocol: netsec.SASLSSL, Mechanism: netsec.MechSCRAMSHA512, Username: "agent", Password: "pw",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	scan, err := c.newKgo(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"t": {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer scan.Close()

	for name, cl := range map[string]*kgo.Client{"admin": c.cl, "scan": scan} {
		mechs, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism)
		if len(mechs) != 1 || mechs[0].Name() != netsec.MechSCRAMSHA512 {
			t.Errorf("%s client: SASL mechanisms = %d, want SCRAM-SHA-512", name, len(mechs))
		}
		if cfg, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config); cfg == nil {
			t.Errorf("%s client: no TLS config", name)
		}
	}
}

func TestNewSecureInvalid(t *testing.T) {
	_, err := NewSecure([]string{"127.0.0.1:1"}, nil, netsec.Kafka{Protocol: netsec.SASLPlaintext, Mechanism: netsec.MechPlain, Username: "u", Password: "p"})
	if !errors.Is(err, netsec.ErrInvalid) {
		t.Fatalf("err = %v, want netsec.ErrInvalid", err)
	}
}

func TestNewKeepsPlaintextDefault(t *testing.T) {
	c, err := New([]string{"127.0.0.1:1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if mechs, _ := c.cl.OptValue(kgo.SASL).([]sasl.Mechanism); len(mechs) != 0 {
		t.Errorf("plain New has SASL mechanisms")
	}
	if cfg, _ := c.cl.OptValue(kgo.DialTLSConfig).(*tls.Config); cfg != nil {
		t.Errorf("plain New has TLS")
	}
}
