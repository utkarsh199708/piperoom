package bigquery

import (
	"strings"
	"testing"
)

func TestParseDSN(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		want    Config
		wantErr string // substring; empty means success
	}{
		{name: "minimal", dsn: "bigquery://acme-data/us",
			want: Config{Project: "acme-data", Location: "us", BillingProject: "acme-data", MaxBytesBilled: DefaultMaxBytesBilled}},
		{name: "region and options", dsn: "bigquery://acme-data/europe-west2?billing_project=acme-billing&max_bytes_billed=1048576",
			want: Config{Project: "acme-data", Location: "europe-west2", BillingProject: "acme-billing", MaxBytesBilled: 1 << 20}},
		{name: "upper-case multi-region folded", dsn: "bigquery://acme-data/EU",
			want: Config{Project: "acme-data", Location: "eu", BillingProject: "acme-data", MaxBytesBilled: DefaultMaxBytesBilled}},
		{name: "zero bytes cap means project default", dsn: "bigquery://acme-data/us?max_bytes_billed=0",
			want: Config{Project: "acme-data", Location: "us", BillingProject: "acme-data", MaxBytesBilled: 0}},
		{name: "scheme case-insensitive", dsn: "BigQuery://acme-data/us",
			want: Config{Project: "acme-data", Location: "us", BillingProject: "acme-data", MaxBytesBilled: DefaultMaxBytesBilled}},

		{name: "wrong scheme", dsn: "postgres://acme-data/us", wantErr: "scheme"},
		{name: "not a url", dsn: "bigquery://acme data/%zz", wantErr: "not a URL"},
		{name: "credentials", dsn: "bigquery://user:secret@acme-data/us", wantErr: "credentials"},
		{name: "no location", dsn: "bigquery://acme-data", wantErr: "no location"},
		{name: "no location slash", dsn: "bigquery://acme-data/", wantErr: "no location"},
		{name: "location with path", dsn: "bigquery://acme-data/us/analytics", wantErr: "location"},
		{name: "location injection", dsn: "bigquery://acme-data/us`;DROP", wantErr: "location"},
		{name: "bad project", dsn: "bigquery://Acme_Data/us", wantErr: "project"},
		{name: "short project", dsn: "bigquery://abc/us", wantErr: "project"},
		{name: "project with port", dsn: "bigquery://acme-data:443/us", wantErr: "project"},
		{name: "bad billing project", dsn: "bigquery://acme-data/us?billing_project=x", wantErr: "billing_project"},
		{name: "unknown param", dsn: "bigquery://acme-data/us?dataset=analytics", wantErr: "not known"},
		{name: "repeated param", dsn: "bigquery://acme-data/us?max_bytes_billed=1&max_bytes_billed=2", wantErr: "repeated"},
		{name: "negative cap", dsn: "bigquery://acme-data/us?max_bytes_billed=-1", wantErr: "max_bytes_billed"},
		{name: "fragment", dsn: "bigquery://acme-data/us#x", wantErr: "fragment"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDSN(tc.dsn)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseDSN(%q) = %+v, want error with %q", tc.dsn, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not mention %q", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatalf("error echoes the credential: %q", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDSN(%q): %v", tc.dsn, err)
			}
			if got != tc.want {
				t.Fatalf("ParseDSN(%q) = %+v, want %+v", tc.dsn, got, tc.want)
			}
		})
	}
}

func TestTargetHasProjectAndLocation(t *testing.T) {
	if got := testCfg.target(); got != "bigquery acme-data/us" {
		t.Fatalf("target = %q", got)
	}
}
