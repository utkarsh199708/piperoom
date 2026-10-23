package testenv

import "testing"

func TestOracleDSNFromEnv(t *testing.T) {
	cases := []struct {
		env, want string
	}{
		{"", DefaultOracleDSN},
		{"   ", DefaultOracleDSN},
		{" oracle://u:p@db:1522/PDB2 ", "oracle://u:p@db:1522/PDB2"},
	}
	for _, tc := range cases {
		got := OracleDSNFromEnv(func(k string) string {
			if k != OracleDSNEnv {
				t.Fatalf("read %q, want %q", k, OracleDSNEnv)
			}
			return tc.env
		})
		if got != tc.want {
			t.Errorf("env %q: got %q, want %q", tc.env, got, tc.want)
		}
	}
}

func TestOracleHost(t *testing.T) {
	cases := []struct{ dsn, want string }{
		{DefaultOracleDSN, "localhost:1521"},
		{"oracle://u:p@10.0.0.5:1522/FREE", "10.0.0.5:1522"},
		{"not a url", ""},
		{"://bad", ""},
	}
	for _, tc := range cases {
		if got := oracleHost(tc.dsn); got != tc.want {
			t.Errorf("oracleHost(%q) = %q, want %q", tc.dsn, got, tc.want)
		}
	}
}
