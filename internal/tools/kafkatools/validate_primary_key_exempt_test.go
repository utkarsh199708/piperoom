package kafkatools

import "testing"

// TestPlaintextSecretKeyPrimaryKey pins KAN-545: the JDBC sink's
// primary.key.* settings name record keys, not credentials, while real secrets
// and the existing exemptions keep their verdict.
func TestPlaintextSecretKeyPrimaryKey(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"primary.key.mode", false},
		{"primary.key.fields", false},
		{"Primary.Key.Mode", false},
		{"transforms.x.primary.key.fields", false},
		{"connection.password", true},
		{"primary.key.password", true},
		{"primary.key.secret", true},
		{"api.key", true},
		{"primary.key", true},
		{"key.ignore", false},
		{"message.key.columns", false},
		{"key.format", false},
		{"key.fields", false},
		{"key.converter", false},
	}
	for _, tc := range cases {
		if got := plaintextSecretKey(tc.key); got != tc.want {
			t.Errorf("plaintextSecretKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}
