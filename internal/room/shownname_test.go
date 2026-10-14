package room

import (
	"testing"

	"piperoom/internal/store/memstore"
)

// LEAD-REVIEW round 1 on #115: the suffix ShownName adds must not repeat the
// forgery it marks. Look-alike letters are escapes so a reader sees them.
func TestShownNameSuffixTag(t *testing.T) {
	svc := New(Deps{Store: memstore.New(), Users: mustUsers([]User{
		{ID: "priya", Name: "Priya", Email: "priya@example.com"},
		{ID: "arjun", Name: "Arjun"},
	})})
	t.Cleanup(svc.Close)
	const id = "u_0123456789ab"
	cases := []struct {
		name, email, want, resembles string
	}{
		// The email is a plain, distinct mailbox: it is the tag.
		{"Priya", "on.call@vendor.example", "Priya (on.call@vendor.example)", "priya"},
		// Cyrillic small a (U+0430) in the email: the u_ id is the tag.
		{"Priya", "priyа@example.com", "Priya (" + id + ")", "priya"},
		// Digit 1 for l: renders like priya's users.yaml email.
		{"Priya", "priya@examp1e.com", "Priya (" + id + ")", "priya"},
		// Verified, ASCII, but the same mailbox as a users.yaml person.
		{"Priya", "PRIYA@example.com", "Priya (" + id + ")", "priya"},
		// No email at all.
		{"Priya", "", "Priya (" + id + ")", "priya"},
		// Bracket and email shapes the first round missed.
		{"Priya <priya@example.com>", "on.call@vendor.example", "Priya <priya@example.com> (on.call@vendor.example)", "priya"},
		{"Priya - priya@example.com", "on.call@vendor.example", "Priya - priya@example.com (on.call@vendor.example)", "suffix-shape"},
		{"Bob «bob@vendor.example»", "on.call@vendor.example", "Bob «bob@vendor.example» (on.call@vendor.example)", "suffix-shape"},
		// Not a look-alike: shown as sent, email unused.
		{"Priyanka Rao", "priyа@example.com", "Priyanka Rao", ""},
	}
	for _, c := range cases {
		shown, resembles := svc.ShownName(id, c.name, c.email)
		if shown != c.want || resembles != c.resembles {
			t.Errorf("ShownName(%q, %q) = %q, %q; want %q, %q", c.name, c.email, shown, resembles, c.want, c.resembles)
		}
	}
}
