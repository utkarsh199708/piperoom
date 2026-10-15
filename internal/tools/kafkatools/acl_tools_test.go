package kafkatools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

type fakeACLs struct {
	*fakeKafka
	entries []clients.ACLEntry
	err     error
	topics  []string
	groups  []string
}

func (f *fakeACLs) ACLs(_ context.Context, topics, groups []string) ([]clients.ACLEntry, error) {
	f.topics, f.groups = topics, groups
	return f.entries, f.err
}

func aclCtx() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{
		ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, ConsumerGroups: []string{"order-sync"},
	})
}

func ent(principal, typ, res, pat, op, perm string) clients.ACLEntry {
	return clients.ACLEntry{Principal: principal, Host: "*", ResourceType: typ, Resource: res, Pattern: pat, Operation: op, Permission: perm}
}

func aclFake() *fakeACLs {
	return &fakeACLs{fakeKafka: &fakeKafka{}, entries: []clients.ACLEntry{
		ent("User:debezium", "TOPIC", "pg.", "PREFIXED", "READ", "ALLOW"),
		ent("User:debezium", "TOPIC", "pg.", "PREFIXED", "DESCRIBE", "ALLOW"),
		ent("User:debezium", "CLUSTER", "kafka-cluster", "LITERAL", "DESCRIBE", "ALLOW"),
		ent("User:sink", "GROUP", "order-sync", "LITERAL", "READ", "ALLOW"),
		ent("User:billing", "CLUSTER", "kafka-cluster", "LITERAL", "ALTER", "ALLOW"),
	}}
}

func TestKafkaACLsFilterToPipeline(t *testing.T) {
	f := aclFake()
	r, err := newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.topics, ",") != "pg.public.orders" || strings.Join(f.groups, ",") != "order-sync" {
		t.Fatalf("asked topics=%v groups=%v", f.topics, f.groups)
	}
	if !strings.Contains(r.Summary, "User:debezium: ALLOW DESCRIBE cluster; ALLOW DESCRIBE,READ topic pg. (PREFIXED)") ||
		!strings.Contains(r.Summary, "User:sink: ALLOW READ group order-sync (LITERAL)") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
	if strings.Contains(r.Summary, "billing") || strings.Contains(r.Summary, "missing:") {
		t.Fatalf("other tenant's principal or verdict without a principal:\n%s", r.Summary)
	}
}

func TestKafkaACLsPrincipalMissingWrite(t *testing.T) {
	f := aclFake()
	r, err := newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{"principal":"User:debezium"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "missing: WRITE on topic pg.public.orders for the connector principal User:debezium") || strings.Contains(r.Summary, "sink") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
	f.entries = append(f.entries, ent("User:debezium", "TOPIC", "pg.public.orders", "LITERAL", "WRITE", "ALLOW"))
	r, _ = newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{"principal":"User:debezium"}`))
	if strings.Contains(r.Summary, "missing:") {
		t.Fatalf("WRITE granted but reported missing:\n%s", r.Summary)
	}
	f.entries = append(f.entries, ent("User:debezium", "TOPIC", "pg.", "PREFIXED", "WRITE", "DENY"))
	r, _ = newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{"principal":"User:debezium"}`))
	if !strings.Contains(r.Summary, "missing: WRITE") {
		t.Fatalf("DENY not honoured:\n%s", r.Summary)
	}
	r, _ = newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{"principal":"User:nobody"}`))
	if !strings.Contains(r.Summary, "no ACLs for User:nobody") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
}

func TestKafkaACLsAuthorizationFailedIsAnswer(t *testing.T) {
	f := aclFake()
	f.err = fmt.Errorf("wrapped: %w", kafka.ErrACLDescribeDenied)
	r, err := newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{}`))
	if err != nil || !strings.Contains(r.Summary, "cannot describe ACLs: the Piperoom principal lacks Describe on Cluster") {
		t.Fatalf("err=%v\n%s", err, r.Summary)
	}
	f.err = kafka.ErrNoAuthorizer
	r, err = newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{}`))
	if err != nil || !strings.Contains(r.Summary, "no ACL authorizer") {
		t.Fatalf("err=%v\n%s", err, r.Summary)
	}
	boom := errors.New("down")
	f.err = boom
	if _, err := newTool(t, f, "kafka_acls").Run(aclCtx(), []byte(`{}`)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestKafkaACLsInput(t *testing.T) {
	tool := newTool(t, aclFake(), "kafka_acls")
	for _, in := range []string{`{"principal":"User:a b"}`, `{"principal":"User:x;drop"}`, `{"principal":"` + strings.Repeat("a", 201) + `"}`, `{"principal":`} {
		if _, err := tool.Run(aclCtx(), []byte(in)); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%.30s: err = %v", in, err)
		}
	}
	if _, err := tool.Run(tools.WithUnscoped(context.Background()), []byte(`{}`)); !errors.Is(err, tools.ErrBadInput) {
		t.Errorf("unscoped: %v", err)
	}
	if _, err := tool.Run(context.Background(), []byte(`{}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Errorf("no scope: %v", err)
	}
	if _, err := (&kafkaACLs{d: Deps{Kafka: &fakeKafka{}}}).Run(aclCtx(), []byte(`{}`)); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("plain client: %v", err)
	}
}

// TestACLSummaryHosts (#304): an ALLOW limited to one host is shown with its
// host and is not a full WRITE grant; a host-specific DENY applies only to
// that host.
func TestACLSummaryHosts(t *testing.T) {
	const topic = "pg.public.orders"
	onHost := func(e clients.ACLEntry, host string) clients.ACLEntry { e.Host = host; return e }
	allowAll := ent("User:debezium", "TOPIC", topic, "LITERAL", "WRITE", "ALLOW")
	for _, tc := range []struct {
		name    string
		entries []clients.ACLEntry
		want    []string
		notWant []string
	}{
		{"allow from one host", []clients.ACLEntry{onHost(allowAll, "10.9.9.9")},
			[]string{"ALLOW WRITE topic pg.public.orders (LITERAL) from 10.9.9.9",
				"WRITE on topic pg.public.orders for User:debezium is allowed only from 10.9.9.9"},
			[]string{"missing: WRITE"}},
		{"deny from one host", []clients.ACLEntry{allowAll, onHost(ent("User:debezium", "TOPIC", topic, "LITERAL", "WRITE", "DENY"), "10.1.1.1")},
			[]string{"DENY WRITE topic pg.public.orders (LITERAL) from 10.1.1.1",
				"WRITE on topic pg.public.orders for User:debezium is denied from 10.1.1.1"},
			[]string{"missing: WRITE"}},
		{"allow and deny on the same host", []clients.ACLEntry{onHost(allowAll, "10.9.9.9"), onHost(ent("User:debezium", "TOPIC", topic, "LITERAL", "ALL", "DENY"), "10.9.9.9")},
			[]string{"missing: WRITE on topic pg.public.orders"}, nil},
		{"deny from every host", []clients.ACLEntry{allowAll, ent("User:debezium", "TOPIC", topic, "LITERAL", "WRITE", "DENY")},
			[]string{"missing: WRITE on topic pg.public.orders"}, []string{" from "}},
		{"full grant", []clients.ACLEntry{allowAll},
			nil, []string{"missing: WRITE", "allowed only", "denied from", " from "}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := aclSummary(tc.entries, []string{topic}, "User:debezium")
			for _, w := range tc.want {
				if !strings.Contains(s, w) {
					t.Errorf("summary lacks %q:\n%s", w, s)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(s, w) {
					t.Errorf("summary has %q:\n%s", w, s)
				}
			}
		})
	}
}
