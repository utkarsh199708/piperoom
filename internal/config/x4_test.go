package config

import (
	"testing"
	"time"
)

// The X4 change webhook secret reaches its field; empty leaves the route off.
func TestX4ChangesWebhookSecret(t *testing.T) {
	t.Setenv("CHANGES_WEBHOOK_SECRET", "")
	if c := Load(); c.ChangesWebhookSecret != "" {
		t.Fatalf("ChangesWebhookSecret = %q, want empty", c.ChangesWebhookSecret)
	}
	t.Setenv("CHANGES_WEBHOOK_SECRET", "new,old")
	if c := Load(); c.ChangesWebhookSecret != "new,old" {
		t.Fatalf("ChangesWebhookSecret = %q", c.ChangesWebhookSecret)
	}
	if ChangesWebhookMaxBodyBytes != 4096 || ChangesWebhookPerMinute != 60 || VendorStatusPollInterval != 5*time.Minute {
		t.Fatal("X4 webhook limits changed; docs/api.md (External changes) states them")
	}
}
