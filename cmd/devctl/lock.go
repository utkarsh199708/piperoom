package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"piperoom/internal/stacklock"
)

// ownerEnv identifies the lock holder for mutating commands when --owner is
// not passed explicitly.
const ownerEnv = "PIPEROOM_STACK_OWNER"

// staleAfter is how old a lock must be before `devctl lock --steal` may remove it.
const staleAfter = 15 * time.Minute

// splitOwner pulls a leading-or-embedded `--owner <tag>` flag out of args and
// falls back to $PIPEROOM_STACK_OWNER. It returns the owner and the remaining
// positional args.
func splitOwner(args []string) (owner string, rest []string, err error) {
	owner = os.Getenv(ownerEnv)
	for i := 0; i < len(args); i++ {
		if args[i] != "--owner" {
			continue
		}
		if i+1 >= len(args) {
			return "", nil, fmt.Errorf("devctl: --owner requires a value")
		}
		owner = args[i+1]
		rest = make([]string, 0, len(args)-2)
		rest = append(rest, args[:i]...)
		rest = append(rest, args[i+2:]...)
		return owner, rest, nil
	}
	return owner, args, nil
}

// requireLock refuses to run a mutating command unless owner currently holds
// the shared stack lock. The returned error always names the current holder.
func requireLock(owner string) error {
	if owner == "" {
		return fmt.Errorf("devctl: this command mutates the shared stack; pass --owner <tag> or set %s", ownerEnv)
	}
	lk := stacklock.New(stacklock.Path())
	rec, err := lk.Status()
	if err != nil {
		return fmt.Errorf("devctl: read stack lock: %w", err)
	}
	if rec == nil {
		return fmt.Errorf("devctl: stack is not locked; run `devctl lock --owner %s` first", owner)
	}
	if rec.Owner != owner {
		return fmt.Errorf("devctl: stack is locked by %q (you are %q); wait for them to run `devctl unlock --owner %s`", rec.Owner, owner, rec.Owner)
	}
	return nil
}

// lockCmd implements `devctl lock [--owner <tag>] [--steal] [status]`.
func lockCmd(args []string) error {
	fs := flag.NewFlagSet("lock", flag.ContinueOnError)
	owner := fs.String("owner", os.Getenv(ownerEnv), "lock owner tag")
	steal := fs.Bool("steal", false, "steal the lock if it is older than 15m")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}

	lk := stacklock.New(stacklock.Path())
	if len(fs.Args()) == 1 && fs.Args()[0] == "status" {
		return printLockStatus(lk)
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("usage: devctl lock [--owner <tag>] [--steal] | status")
	}

	if *steal {
		rec, err := lk.Steal(staleAfter)
		if err != nil {
			return fmt.Errorf("devctl: steal lock: %w", err)
		}
		fmt.Printf("stole stack lock from %q (held since %s)\n", rec.Owner, rec.At.Format(time.RFC3339))
		return nil
	}
	if *owner == "" {
		return fmt.Errorf("devctl: lock requires --owner <tag> or %s", ownerEnv)
	}
	rec, err := lk.Acquire(*owner)
	if err != nil {
		return fmt.Errorf("devctl: lock: %w", err)
	}
	fmt.Printf("stack locked by %q (since %s)\n", rec.Owner, rec.At.Format(time.RFC3339))
	return nil
}

// unlockCmd implements `devctl unlock --owner <tag>`.
func unlockCmd(args []string) error {
	fs := flag.NewFlagSet("unlock", flag.ContinueOnError)
	owner := fs.String("owner", os.Getenv(ownerEnv), "lock owner tag")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("usage: devctl unlock --owner <tag>")
	}
	if *owner == "" {
		return fmt.Errorf("devctl: unlock requires --owner <tag> or %s", ownerEnv)
	}
	lk := stacklock.New(stacklock.Path())
	if err := lk.Release(*owner); err != nil {
		return fmt.Errorf("devctl: unlock: %w", err)
	}
	fmt.Printf("stack lock released by %q\n", *owner)
	return nil
}

func printLockStatus(lk *stacklock.Lock) error {
	rec, err := lk.Status()
	if err != nil {
		return fmt.Errorf("devctl: read stack lock: %w", err)
	}
	if rec == nil {
		fmt.Printf("stack lock: not locked (%s)\n", lk.Path())
		return nil
	}
	fmt.Printf("stack lock: held by %q since %s (%s ago)\n",
		rec.Owner, rec.At.Format(time.RFC3339), time.Since(rec.At).Round(time.Second))
	return nil
}
