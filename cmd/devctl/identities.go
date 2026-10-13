package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/store"
)

// identitiesCmd runs `devctl identities unbind <user>` (docs/design/auth.md
// section 2.1). It deletes the user's identities row in the room database
// (PIPEROOM_DB_URL), so the next verified-email login binds the new IdP
// account. Use it when a person moves to a new IdP account, or after a login was
// refused because the email is bound to another account.
func identitiesCmd(ctx context.Context, st *stack, args []string, out io.Writer) error {
	if len(args) != 2 || args[0] != "unbind" || args[1] == "" {
		return fmt.Errorf("usage: devctl identities unbind <user>")
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pg, err := store.New(cctx, st.cfg.RoomDBURL, st.log)
	if err != nil {
		return fmt.Errorf("devctl: connect room database: %w", err)
	}
	defer pg.Close()
	return unbindIdentity(cctx, pg.Identities(), args[1], out)
}

// unbindIdentity deletes the user's binding and says what it did.
func unbindIdentity(ctx context.Context, ids auth.IdentityStore, user string, out io.Writer) error {
	n, err := ids.Unbind(ctx, user)
	if err != nil {
		return fmt.Errorf("devctl: unbind %s: %w", user, err)
	}
	if n == 0 {
		_, _ = fmt.Fprintf(out, "no identity was bound to %q; nothing changed\n", user)
		return nil
	}
	_, _ = fmt.Fprintf(out, "unbound %q; the next login with a verified matching email binds the new account\n", user)
	return nil
}
