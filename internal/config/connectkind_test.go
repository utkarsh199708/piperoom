package config_test

import (
	"strings"
	"testing"

	"piperoom/internal/config"
)

func TestParseConnectKind(t *testing.T) {
	cases := []struct {
		name    string
		c       config.Config
		want    string
		wantErr string
	}{
		{"empty is rest", config.Config{}, "rest", ""},
		{"rest", config.Config{ConnectKind: "rest", ConnectPassword: "x"}, "rest", ""},
		{"case and space", config.Config{ConnectKind: " MSK ", AWSRegion: "eu-west-1"}, "msk", ""},
		{"confluent", config.Config{ConnectKind: "confluent"}, "confluent", ""},
		{"aiven", config.Config{ConnectKind: "aiven"}, "aiven", ""},
		{"msk without region", config.Config{ConnectKind: "msk"}, "", "AWS_REGION"},
		{"msk with rest login", config.Config{ConnectKind: "msk", AWSRegion: "eu-west-1", ConnectUsername: "u"}, "", "CONNECT_USERNAME"},
		{"msk with tls", config.Config{ConnectKind: "msk", AWSRegion: "eu-west-1", ConnectTLSCAFile: "/ca"}, "", "CONNECT_TLS_CA_FILE"},
		{"unknown", config.Config{ConnectKind: "strimzi"}, "", "CONNECT_KIND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := config.ParseConnectKind(tc.c)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
