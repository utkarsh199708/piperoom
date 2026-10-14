package main

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"piperoom/internal/auth"
	"piperoom/internal/config"
)

func testLog() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})), &buf
}

func devCfg(addr string) config.Config {
	return config.Config{AuthMode: "dev", HTTPAddr: addr}
}

// Dev mode on 0.0.0.0 without the switch refuses to start.
func TestAuthSetupRefusesNonLoopbackWithoutSwitch(t *testing.T) {
	log, _ := testLog()
	for _, addr := range []string{"0.0.0.0:8090", ":8090", "192.168.1.5:8090", "[::]:8090"} {
		_, err := authSetup(devCfg(addr), false, false, log)
		if !errors.Is(err, errNonLoopback) {
			t.Fatalf("addr %q: want errNonLoopback, got %v", addr, err)
		}
		if !strings.Contains(err.Error(), "PIPEROOM_DEV_ALLOW_NONLOOPBACK=1") {
			t.Fatalf("addr %q: the error should name the switch: %v", addr, err)
		}
	}
	// Only "1" counts as the switch.
	cfg := devCfg("0.0.0.0:8090")
	cfg.DevAllowNonLoopback = "true"
	if _, err := authSetup(cfg, false, false, log); !errors.Is(err, errNonLoopback) {
		t.Fatalf("switch=true: want errNonLoopback, got %v", err)
	}
}

// With the switch, or the old flag, dev mode starts and warns loudly.
func TestAuthSetupWarnsWithSwitchOrFlag(t *testing.T) {
	t.Run("env switch", func(t *testing.T) {
		log, buf := testLog()
		cfg := devCfg("0.0.0.0:8090")
		cfg.DevAllowNonLoopback = "1"
		cfg.DevAck = config.DevAckValue
		s, err := authSetup(cfg, false, false, log)
		if err != nil {
			t.Fatal(err)
		}
		if !s.DevNonLoopback || s.Mode != auth.ModeDev {
			t.Fatalf("settings = %+v", s)
		}
		out := buf.String()
		if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "dev auth on a non-loopback address") {
			t.Fatalf("no loud warning:\n%s", out)
		}
		if strings.Contains(out, "deprecated") {
			t.Fatalf("env switch should not log the flag deprecation:\n%s", out)
		}
	})
	t.Run("old flag", func(t *testing.T) {
		log, buf := testLog()
		cfg := devCfg("0.0.0.0:8090")
		cfg.DevAck = config.DevAckValue
		s, err := authSetup(cfg, true, false, log)
		if err != nil {
			t.Fatal(err)
		}
		if !s.DevNonLoopback {
			t.Fatalf("settings = %+v", s)
		}
		out := buf.String()
		if !strings.Contains(out, "--allow-remote is deprecated") || !strings.Contains(out, "PIPEROOM_DEV_ALLOW_NONLOOPBACK=1") {
			t.Fatalf("no deprecation warning:\n%s", out)
		}
		if !strings.Contains(out, "dev auth on a non-loopback address") {
			t.Fatalf("no loud warning:\n%s", out)
		}
	})
	t.Run("loopback needs nothing and stays quiet", func(t *testing.T) {
		log, buf := testLog()
		cfg := devCfg("127.0.0.1:8090")
		cfg.DevAllowNonLoopback = "1"
		s, err := authSetup(cfg, false, false, log)
		if err != nil {
			t.Fatal(err)
		}
		if s.DevNonLoopback || strings.Contains(buf.String(), "non-loopback") {
			t.Fatalf("loopback listen should not warn: %+v\n%s", s, buf.String())
		}
	})
}

func TestAuthSetupModeMustBeDevOrOIDC(t *testing.T) {
	log, _ := testLog()
	for _, mode := range []string{"proxy", "none", "oauth"} {
		cfg := devCfg("127.0.0.1:8090")
		cfg.AuthMode = mode
		_, err := authSetup(cfg, false, false, log)
		if err == nil || !strings.Contains(err.Error(), "PIPEROOM_AUTH_MODE") {
			t.Fatalf("mode %q: want an error naming PIPEROOM_AUTH_MODE, got %v", mode, err)
		}
	}
	// Empty means the default, dev.
	cfg := devCfg("127.0.0.1:8090")
	cfg.AuthMode = ""
	if s, err := authSetup(cfg, false, false, log); err != nil || s.Mode != auth.ModeDev {
		t.Fatalf("empty mode: %+v, %v", s, err)
	}
}

func oidcCfg() config.Config {
	return config.Config{
		AuthMode:         "oidc",
		HTTPAddr:         "0.0.0.0:8090",
		PublicURL:        "https://piperoom.corp.example",
		OIDCIssuer:       "https://idp.corp.example",
		OIDCClientID:     "piperoom",
		OIDCClientSecret: "s3cret",
	}
}

func TestAuthSetupOIDCValidation(t *testing.T) {
	log, _ := testLog()
	if s, err := authSetup(oidcCfg(), false, false, log); err != nil || !s.IsOIDC() {
		t.Fatalf("complete oidc config: %+v, %v", s, err)
	}
	cases := []struct {
		name   string
		mutate func(*config.Config)
		want   string
	}{
		{"no public url", func(c *config.Config) { c.PublicURL = "" }, "PIPEROOM_PUBLIC_URL"},
		{"http public url", func(c *config.Config) { c.PublicURL = "http://piperoom.corp.example" }, "https"},
		{"no issuer", func(c *config.Config) { c.OIDCIssuer = "" }, "PIPEROOM_OIDC_ISSUER"},
		{"no client id", func(c *config.Config) { c.OIDCClientID = "" }, "PIPEROOM_OIDC_CLIENT_ID"},
		{"no secret", func(c *config.Config) { c.OIDCClientSecret = "" }, "PIPEROOM_OIDC_CLIENT_SECRET"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := oidcCfg()
			tc.mutate(&cfg)
			_, err := authSetup(cfg, false, false, log)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error naming %s, got %v", tc.want, err)
			}
		})
	}
	// A public client may leave the secret empty.
	cfg := oidcCfg()
	cfg.OIDCClientSecret = ""
	cfg.OIDCPublicClient = "1"
	if _, err := authSetup(cfg, false, false, log); err != nil {
		t.Fatalf("public client without secret: %v", err)
	}
	// The secret never appears in an error.
	cfg = oidcCfg()
	cfg.OIDCIssuer = ""
	if _, err := authSetup(cfg, false, false, log); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("error leaks the secret or is nil: %v", err)
	}
}

// oidc mode has no loopback guard (OIDC is the guard) and ignores the dev
// switches with a warning; --demo forces dev.
func TestAuthSetupOIDCIgnoresDevSwitchesAndDemoForcesDev(t *testing.T) {
	log, buf := testLog()
	cfg := oidcCfg()
	cfg.DevAllowNonLoopback = "1"
	s, err := authSetup(cfg, true, false, log)
	if err != nil || !s.IsOIDC() || s.DevNonLoopback {
		t.Fatalf("oidc on 0.0.0.0: %+v, %v", s, err)
	}
	if !strings.Contains(buf.String(), "ignored in oidc mode") {
		t.Fatalf("no ignored-switch warning:\n%s", buf.String())
	}

	log, buf = testLog()
	s, err = authSetup(oidcCfg(), false, true, log)
	_ = s
	// --demo with oidc config and a non-loopback address: forced to dev, which
	// then refuses the address.
	if !errors.Is(err, errNonLoopback) {
		t.Fatalf("demo + oidc + 0.0.0.0: want errNonLoopback from the forced dev mode, got %v", err)
	}
	if !strings.Contains(buf.String(), "--demo forces PIPEROOM_AUTH_MODE=dev") {
		t.Fatalf("no demo warning:\n%s", buf.String())
	}
}

func TestAuthSetupParsesSessionsAndHosts(t *testing.T) {
	log, _ := testLog()
	cfg := devCfg("127.0.0.1:8090")
	cfg.SessionIdle, cfg.SessionMax = "30m", "2h"
	cfg.AllowedHosts = " Piperoom.Internal:8090 , ,gw.example "
	s, err := authSetup(cfg, false, false, log)
	if err != nil {
		t.Fatal(err)
	}
	if s.Idle.String() != "30m0s" || s.Max.String() != "2h0m0s" {
		t.Fatalf("durations = %v / %v", s.Idle, s.Max)
	}
	if len(s.AllowedHosts) != 2 || s.AllowedHosts[0] != "piperoom.internal:8090" || s.AllowedHosts[1] != "gw.example" {
		t.Fatalf("allowed hosts = %v", s.AllowedHosts)
	}
	for _, bad := range []struct{ idle, max, hosts string }{
		{"soon", "", ""}, {"", "0s", ""}, {"", "-1h", ""}, {"", "", "*.example.com"}, {"", "", "a/b"},
	} {
		c := devCfg("127.0.0.1:8090")
		c.SessionIdle, c.SessionMax, c.AllowedHosts = bad.idle, bad.max, bad.hosts
		if _, err := authSetup(c, false, false, log); err == nil {
			t.Fatalf("%+v: want an error", bad)
		}
	}
}

// Off loopback the switch is not enough: the server also needs the written
// acknowledgement, exactly config.DevAckValue (R0 contract, enforced here).
func TestAuthSetupNeedsDevAck(t *testing.T) {
	for _, tc := range []struct {
		name, ack string
		flag      bool
		want      error
	}{
		{name: "switch without ack", want: errDevAckMissing},
		{name: "flag without ack", flag: true, want: errDevAckMissing},
		{name: "ack is 1", ack: "1"},
		{name: "ack is true", ack: "true"},
		{name: "ack with other text", ack: "dev-login-has-no-password!"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, _ := testLog()
			cfg := devCfg("0.0.0.0:8090")
			if !tc.flag {
				cfg.DevAllowNonLoopback = "1"
			}
			cfg.DevAck = tc.ack
			_, err := authSetup(cfg, tc.flag, false, log)
			if err == nil {
				t.Fatal("started without a valid PIPEROOM_DEV_ACK")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if !strings.Contains(err.Error(), "PIPEROOM_DEV_ACK") {
				t.Fatalf("the error should name PIPEROOM_DEV_ACK: %v", err)
			}
		})
	}
	t.Run("ack with spaces is accepted", func(t *testing.T) {
		log, _ := testLog()
		cfg := devCfg("0.0.0.0:8090")
		cfg.DevAllowNonLoopback = "1"
		cfg.DevAck = "  " + config.DevAckValue + " "
		if _, err := authSetup(cfg, false, false, log); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("loopback needs no ack", func(t *testing.T) {
		log, _ := testLog()
		if _, err := authSetup(devCfg("127.0.0.1:8090"), false, false, log); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("demo needs no ack", func(t *testing.T) {
		log, _ := testLog()
		cfg := devCfg("0.0.0.0:8090")
		cfg.DevAllowNonLoopback = "1"
		if _, err := authSetup(cfg, false, true, log); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("oidc ignores a bad ack with a warning", func(t *testing.T) {
		log, buf := testLog()
		cfg := oidcCfg()
		cfg.DevAck = "1"
		if _, err := authSetup(cfg, false, false, log); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(buf.String(), "PIPEROOM_DEV_ACK") || !strings.Contains(buf.String(), "ignored in oidc mode") {
			t.Fatalf("no ignore warning:\n%s", buf.String())
		}
	})
}
