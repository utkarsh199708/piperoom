package room

import (
	"strings"
	"testing"

	"piperoom/internal/owners"
)

func TestValidateUsers(t *testing.T) {
	devOwners := &owners.Owners{
		Pipelines: []owners.Pipeline{{ID: "orders"}},
		Layers: map[string]string{
			"postgres": "priya", "kafka-connect": "arjun",
			"schema-registry": "priya", "kafka": "arjun",
			"consumer": "kabir", "warehouse": "meera",
		},
	}
	tests := []struct {
		name    string
		users   *Users
		owners  *owners.Owners
		wantErr string // substring, "" means no error
	}{
		{
			name:   "dev defaults match dev owners",
			users:  DefaultUsers(),
			owners: devOwners,
		},
		{
			name:    "users only: unknown layer refused",
			users:   mustUsers([]User{{ID: "a", Name: "A", Layers: []string{"postgres", "nope"}}}),
			owners:  nil,
			wantErr: `unknown layer`,
		},
		{
			name: "owners layer owner missing from users",
			users: mustUsers([]User{
				{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka"}},
				{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}},
				{ID: "meera", Name: "Meera", Layers: []string{"warehouse"}},
			}),
			owners:  devOwners,
			wantErr: `"priya", who is not in the users file`,
		},
		{
			name: "layer mismatch refused",
			users: mustUsers([]User{
				{ID: "priya", Name: "Priya", Layers: []string{"postgres", "warehouse"}},
				{ID: "arjun", Name: "Arjun", Layers: []string{"kafka-connect", "kafka"}},
				{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}},
				{ID: "meera", Name: "Meera", Layers: []string{"warehouse"}},
			}),
			owners:  devOwners,
			wantErr: `"priya" owns layer "schema-registry" in the owners file but lists layers [postgres warehouse]`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateUsers(tc.users, tc.owners)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateUsers: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
