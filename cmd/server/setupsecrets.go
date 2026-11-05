package main

// PIPEROOM_SECRETS_DIR is where the setup rooms' registered read credentials
// land (<dir>/<pipeline>/<name>, 0600; docs/design/setup-agent.md §3.7). Every
// replica with setup on writes there, so a replica that cannot write the
// directory refuses to start instead of accepting a driver's click and failing
// every registration.

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// checkSetupSecretsDir proves dir is writable before the server listens. The
// directory is created (0700) when it does not exist, so an operator's fresh
// named volume at the mount point works; anything that cannot be created or
// written — a regular file at the path, a read-only mount, a full disk — fails
// the start with an error that names the variable, never a secret.
func checkSetupSecretsDir(dir string) error {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return errors.New("PIPEROOM_SETUP_ENABLED=on requires PIPEROOM_SECRETS_DIR: it is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s is not a writable directory: %w", dir, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s: %w", dir, err)
	}
	defer root.Close()
	name, err := probeFileName()
	if err != nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s: %w", dir, err)
	}
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s is not writable: %w", dir, err)
	}
	_, werr := f.WriteString("piperoom write probe\n")
	cerr := f.Close()
	_ = root.Remove(name)
	if werr != nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s is not writable: %w", dir, werr)
	}
	if cerr != nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s: %w", dir, cerr)
	}
	// The probe must really be gone; a leftover would show up in the
	// credentials directory an operator inspects.
	if _, err := root.Lstat(name); err == nil {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s: the write probe could not be removed", dir)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("PIPEROOM_SECRETS_DIR=%s: %w", dir, err)
	}
	return nil
}

// probeFileName is a name no secret can take (it holds dots and dashes), so a
// probe can never overwrite a registered credential.
func probeFileName() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("write probe name: %w", err)
	}
	return ".piperoom-write-probe-" + hex.EncodeToString(b[:]), nil
}
