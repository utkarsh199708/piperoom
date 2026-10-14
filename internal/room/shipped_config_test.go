package room_test

import (
	"testing"

	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// Every committed users/owners pair must keep passing ValidateUsers, so a
// stricter start-up check cannot silently break dev, the Helm example or the
// OIDC e2e stack.
func TestShippedConfigPairsValidate(t *testing.T) {
	pairs := [][2]string{
		{"../../dev/users.yaml", "../../dev/owners.yaml"},
		{"../../deploy/config/users.example.yaml", "../../deploy/config/owners.example.yaml"},
		{"../../test/e2e/oidc/config/users.yaml", "../../test/e2e/oidc/config/owners.yaml"},
	}
	for _, p := range pairs {
		users, err := room.LoadUsers(p[0])
		if err != nil {
			t.Errorf("%s: %v", p[0], err)
			continue
		}
		own, err := owners.Load(p[1])
		if err != nil {
			t.Errorf("%s: %v", p[1], err)
			continue
		}
		if err := room.ValidateUsers(users, own); err != nil {
			t.Errorf("%s + %s: %v", p[0], p[1], err)
		}
	}
	// The discover draft has TODO owners by design (no users file pairs with
	// it); it only has to parse.
	if _, err := owners.Load("../discover/testdata/draft.golden.yaml"); err != nil {
		t.Errorf("discover golden: %v", err)
	}
}
