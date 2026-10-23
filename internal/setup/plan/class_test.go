package plan

import (
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/events"
)

// TestClassify covers each rule of design §3.5: the default, the local
// sources, the receipt clause, the production sources and the person's floor
// that can raise but never lower.
func TestClassify(t *testing.T) {
	tests := []struct {
		name   string
		facts  Facts
		want   string
		reason string
	}{
		{name: "nothing known is shared", facts: Facts{}, want: ClassShared, reason: ReasonDefault},
		{name: "loopback is local", facts: Facts{Loopback: true}, want: ClassLocal, reason: ReasonLoopback},
		{name: "local docker engine is local", facts: Facts{LocalEngine: true}, want: ClassLocal, reason: ReasonLoopback},
		{name: "compose service under the project dir is local", facts: Facts{ComposeService: true}, want: ClassLocal, reason: ReasonLoopback},
		{
			name:  "a database this room created on loopback is local by receipt",
			facts: Facts{CreatedByRoom: true, Loopback: true},
			want:  ClassLocal, reason: ReasonCreatedByRoom,
		},
		{
			name:  "a receipt alone never makes a remote host local",
			facts: Facts{CreatedByRoom: true},
			want:  ClassShared, reason: ReasonDefault,
		},
		{name: "a prod pattern is production", facts: Facts{ProdPattern: true}, want: ClassProduction, reason: ReasonProdPattern},
		{name: "a prod pattern beats loopback", facts: Facts{ProdPattern: true, Loopback: true}, want: ClassProduction, reason: ReasonProdPattern},
		{
			name:  "the person can say production",
			facts: Facts{Person: ClassProduction},
			want:  ClassProduction, reason: ReasonPerson,
		},
		{
			name:  "the person can raise shared to production on a loopback target",
			facts: Facts{Person: ClassProduction, Loopback: true},
			want:  ClassProduction, reason: ReasonPerson,
		},
		{
			name:  "the person can raise local to shared",
			facts: Facts{Person: ClassShared, Loopback: true},
			want:  ClassShared, reason: ReasonPerson,
		},
		{
			name:  "the person cannot lower shared to local",
			facts: Facts{Person: ClassLocal},
			want:  ClassShared, reason: ReasonDefault,
		},
		{
			name:  "the person cannot lower production to shared",
			facts: Facts{Person: ClassShared, ProdPattern: true},
			want:  ClassProduction, reason: ReasonProdPattern,
		},
		{
			name:  "a person saying production with a prod pattern stays production",
			facts: Facts{Person: ClassProduction, ProdPattern: true},
			want:  ClassProduction, reason: ReasonProdPattern,
		},
		{
			name:  "an unknown person class is refused",
			facts: Facts{Person: "prod"},
			want:  "",
		},
		{
			name:  "a person class differing only in case is refused",
			facts: Facts{Person: "Production"},
		},
		{
			name:  "an unknown person class is refused even with a prod pattern",
			facts: Facts{Person: "staging", ProdPattern: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, reason, err := Classify(tt.facts)
			if tt.want == "" {
				if !errors.Is(err, ErrClass) {
					t.Fatalf("Classify(%+v) error = %v, want ErrClass", tt.facts, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Classify(%+v): %v", tt.facts, err)
			}
			if class != tt.want || reason != tt.reason {
				t.Errorf("Classify(%+v) = %s/%s, want %s/%s", tt.facts, class, reason, tt.want, tt.reason)
			}
		})
	}
}

func TestRaise(t *testing.T) {
	tests := []struct {
		floor, person string
		want          string
		wantErr       bool
	}{
		{floor: ClassLocal, person: "", want: ClassLocal},
		{floor: ClassShared, person: "", want: ClassShared},
		{floor: ClassProduction, person: "", want: ClassProduction},
		{floor: ClassLocal, person: ClassLocal, want: ClassLocal},
		{floor: ClassLocal, person: ClassShared, want: ClassShared},
		{floor: ClassLocal, person: ClassProduction, want: ClassProduction},
		{floor: ClassShared, person: ClassLocal, want: ClassShared},
		{floor: ClassShared, person: ClassShared, want: ClassShared},
		{floor: ClassShared, person: ClassProduction, want: ClassProduction},
		{floor: ClassProduction, person: ClassLocal, want: ClassProduction},
		{floor: ClassProduction, person: ClassShared, want: ClassProduction},
		{floor: ClassProduction, person: ClassProduction, want: ClassProduction},
		{floor: "", person: ClassLocal, wantErr: true},
		{floor: "LOCAL", person: ClassLocal, wantErr: true},
		{floor: ClassLocal, person: "prod", wantErr: true},
	}
	for _, tt := range tests {
		got, err := Raise(tt.floor, tt.person)
		if tt.wantErr {
			if !errors.Is(err, ErrClass) {
				t.Errorf("Raise(%q, %q) error = %v, want ErrClass", tt.floor, tt.person, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("Raise(%q, %q): %v", tt.floor, tt.person, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Raise(%q, %q) = %q, want %q", tt.floor, tt.person, got, tt.want)
		}
	}
}

func TestValidClassAndRank(t *testing.T) {
	for class, want := range map[string]int{ClassLocal: 0, ClassShared: 1, ClassProduction: 2} {
		if !ValidClass(class) {
			t.Errorf("ValidClass(%q) = false", class)
		}
		if rank, ok := ClassRank(class); !ok || rank != want {
			t.Errorf("ClassRank(%q) = %d, %v; want %d, true", class, rank, ok, want)
		}
	}
	for _, class := range []string{"", "prod", "Production", "LOCAL"} {
		if ValidClass(class) {
			t.Errorf("ValidClass(%q) = true", class)
		}
		if _, ok := ClassRank(class); ok {
			t.Errorf("ClassRank(%q) ok", class)
		}
	}
}

// actionEvent builds one event whose payload is p.
func actionEvent(t *testing.T, typ events.Type, p events.ActionExecutedPayload) events.Event {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return events.Event{Type: typ, Payload: raw}
}

// TestCreatedByRoom covers the receipts-only rule: only a successful
// action.executed whose receipt names the target counts.
func TestCreatedByRoom(t *testing.T) {
	receipt := &events.SetupReceipt{Kind: "exec", Target: "db.internal:5432", ExitCode: 0}
	tests := []struct {
		name   string
		events []events.Event
		target string
		want   bool
	}{
		{name: "no events", want: false},
		{name: "empty target", events: []events.Event{actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true, Receipt: receipt})}, target: "", want: false},
		{
			name:   "a successful step that reached the target",
			events: []events.Event{actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true, Receipt: receipt})},
			target: "db.internal:5432", want: true,
		},
		{
			name:   "a receipt for another target",
			events: []events.Event{actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true, Receipt: receipt})},
			target: "other.internal:5432", want: false,
		},
		{
			name:   "a failed step",
			events: []events.Event{actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: false, Receipt: receipt})},
			target: "db.internal:5432", want: false,
		},
		{
			name:   "a step with an unknown outcome",
			events: []events.Event{actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true, OutcomeUnknown: true, Receipt: receipt})},
			target: "db.internal:5432", want: false,
		},
		{
			name:   "a step without a receipt",
			events: []events.Event{actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true})},
			target: "db.internal:5432", want: false,
		},
		{
			name:   "the same payload under another event type does not count",
			events: []events.Event{actionEvent(t, events.SetupPlan, events.ActionExecutedPayload{OK: true, Receipt: receipt})},
			target: "db.internal:5432", want: false,
		},
		{
			name: "one good receipt among bad ones",
			events: []events.Event{
				actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: false, Receipt: receipt}),
				{Type: events.ActionExecuted, Payload: json.RawMessage(`not json`)},
				actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true, Receipt: &events.SetupReceipt{Target: "elsewhere"}}),
				actionEvent(t, events.ActionExecuted, events.ActionExecutedPayload{OK: true, Receipt: receipt}),
			},
			target: "db.internal:5432", want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CreatedByRoom(tt.events, tt.target); got != tt.want {
				t.Errorf("CreatedByRoom(%q) = %v, want %v", tt.target, got, tt.want)
			}
		})
	}
}

func TestMatchProdPattern(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		subject  string
		want     bool
	}{
		{name: "no patterns", subject: "db.acme.com", want: false},
		{name: "host glob", patterns: []string{"db-*.acme.com"}, subject: "db-1.acme.com", want: true},
		{name: "host glob miss", patterns: []string{"db-*.acme.com"}, subject: "db-1.acme.net", want: false},
		{name: "case-insensitive host", patterns: []string{"DB-*.acme.com"}, subject: "db-1.ACME.com", want: true},
		{name: "kubectl context keeps its slash literal", patterns: []string{"*/prod"}, subject: "gke_acme_eu-west1/prod", want: true},
		{name: "account id exact", patterns: []string{"123456789012"}, subject: "123456789012", want: true},
		{name: "account id is not a prefix match", patterns: []string{"123456789012"}, subject: "1234567890123", want: false},
		{name: "question mark is one character", patterns: []string{"db?.acme.com"}, subject: "db1.acme.com", want: true},
		{name: "question mark does not match two", patterns: []string{"db?.acme.com"}, subject: "db12.acme.com", want: false},
		{name: "star spans dots", patterns: []string{"*acme.com"}, subject: "db.eu.acme.com", want: true},
		{name: "empty entry matches nothing", patterns: []string{"", "  "}, subject: "db.acme.com", want: false},
		{name: "a trailing FQDN dot is the same host", patterns: []string{"db-*.acme.com"}, subject: "db-1.acme.com.", want: true},
		{name: "a host:port spelling is the same host", patterns: []string{"db-*.acme.com"}, subject: "db-1.acme.com:5432", want: true},
		{name: "a URL spelling is the same host", patterns: []string{"db-*.acme.com"}, subject: "postgres://admin@db-1.acme.com:5432/orders", want: true},
		{name: "a bare IPv6 literal survives whole", patterns: []string{"2001:db8::*"}, subject: "2001:db8::1", want: true},
		{name: "a bracketed IPv6 literal with a port is the same address", patterns: []string{"2001:db8::*"}, subject: "[2001:db8::1]:5432", want: true},
		{name: "a hostname without a port survives whole", patterns: []string{"db-1.acme.com"}, subject: "db-1.acme.com", want: true},
		{name: "normalization widens matching, never the pattern", patterns: []string{"db-1.acme.com:5432"}, subject: "db-1.acme.com:9999", want: false},
		{name: "empty subject", patterns: []string{"*"}, subject: "", want: false},
		{name: "one entry among many", patterns: []string{"x", "", "db.acme.com"}, subject: "db.acme.com", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchProdPattern(tt.patterns, tt.subject); got != tt.want {
				t.Errorf("MatchProdPattern(%q, %q) = %v, want %v", tt.patterns, tt.subject, got, tt.want)
			}
		})
	}
}
