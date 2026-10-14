package spec

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients/netsec"
)

func TestUniqueKey(t *testing.T) {
	tests := []struct {
		rec  Integration
		want string
	}{
		{Integration{Category: CategoryKafka, Kind: "kafka-confluent-cloud"}, "kafka"},
		{Integration{Category: CategoryAlerts, Kind: "alerts-grafana"}, "alerts:alerts-grafana"},
		{Integration{Category: CategorySecretManager, Kind: "secrets-vault", Config: map[string]any{"name": "vault-corp"}}, "secret_manager:vault-corp"},
	}
	for _, tc := range tests {
		if got := UniqueKey(tc.rec); got != tc.want {
			t.Errorf("UniqueKey(%v) = %q, want %q", tc.rec.Category, got, tc.want)
		}
	}
}

func TestToCatalogNoNulls(t *testing.T) {
	b, err := json.Marshal(ToCatalog(Kind{ID: "x", Category: CategoryConnect, Fields: []Field{{Name: "url", Type: FieldText}}}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "null") {
		t.Fatalf("catalog JSON has null: %s", b)
	}
}

func TestValuesNeverPrintSecret(t *testing.T) {
	v := Values{Secrets: map[string]netsec.Secret{"password": "canary-123"}}
	b, _ := json.Marshal(v)
	if strings.Contains(string(b), "canary-123") {
		t.Fatalf("secret in JSON: %s", b)
	}
}

func TestFailedAndIsSecret(t *testing.T) {
	if Failed([]Check{{Status: CheckWarn}, {Status: CheckSkipped}}) {
		t.Error("warn/skipped must not fail")
	}
	if !Failed([]Check{{Status: CheckPass}, {Status: CheckFail}}) {
		t.Error("fail must fail")
	}
	for ft, want := range map[FieldType]bool{FieldSecret: true, FieldPEM: true, FieldJSONFile: true, FieldText: false, FieldNumber: false} {
		if ft.IsSecret() != want {
			t.Errorf("%s.IsSecret() != %v", ft, want)
		}
	}
	if !CategorySecretManager.Valid() || Category("nope").Valid() {
		t.Error("Valid")
	}
}
