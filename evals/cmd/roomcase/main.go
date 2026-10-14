// Command roomcase turns one closed, labelled room into a pending eval scenario.
//
//	go run ./evals/cmd/roomcase -export room.json -owners owners.yaml -out DIR [-force]
//
// room.json is a GET /api/rooms/{id}/export document; its integrity hash is
// verified first. Nothing else is read: no database, no network, so this runs
// on any laptop. The scenario lands in DIR/pending/, which evalrun never
// replays; a person reviews it and moves it up into DIR. DIR is required and
// must be outside this repository's working tree: the repository is public and
// a learned scenario, even redacted, describes a customer's systems
// (evals/README.md, "Learned scenarios").
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"piperoom/evals"
	"piperoom/internal/owners"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "roomcase:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("roomcase", flag.ContinueOnError)
	fs.SetOutput(stderr)
	export := fs.String("export", "", "room export JSON (GET /api/rooms/{id}/export)")
	own := fs.String("owners", "", "owners.yaml with the room's pipeline")
	out := fs.String("out", "", "private learned scenarios directory (required, outside this repository); the file goes to its pending/ subdirectory")
	force := fs.Bool("force", false, "allow a fingerprint that already exists and overwrite the output file (the per-pipeline cap stays)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *export == "" || *own == "" || *out == "" {
		return errors.New("-export, -owners and -out are required (-out is a private directory outside this repository; there is no default)")
	}
	if err := refuseInsideRepo(*out); err != nil {
		return err
	}
	raw, err := readCapped(*export)
	if err != nil {
		return err
	}
	o, err := owners.Load(*own)
	if err != nil {
		return fmt.Errorf("load owners: %w", err)
	}
	res, path, err := evals.ExportLearned(raw, evals.LearnedOptions{Owners: o, Dir: *out, Force: *force, Validate: true})
	if err != nil {
		return err
	}
	kind := "scenario"
	if res.Hard {
		kind = "hard case (every verdict disputed)"
	}
	fmt.Fprintf(stdout, "wrote %s: %s\nreview every summary and input before moving it out of pending/\n", path, kind)
	return nil
}

// refuseInsideRepo refuses a directory inside the working tree this command
// runs in. The tree is `git rev-parse --show-toplevel` when git answers, else
// the nearest ancestor holding go.mod. Symlinks are resolved on both sides, so
// a link into the tree is no way round; a directory that does not exist yet is
// judged by its nearest existing ancestor.
func refuseInsideRepo(dir string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve -out: %w", err)
	}
	probe := abs
	for {
		if _, err := os.Stat(probe); err == nil {
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			break
		}
		probe = parent
	}
	real, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return fmt.Errorf("resolve -out: %w", err)
	}
	if rel, err := filepath.Rel(root, real); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("-out %s is inside the repository %s, which is public: learned scenarios describe customers' systems and must live in a private directory", dir, root)
	}
	return nil
}

func repoRoot() (string, error) {
	var root string
	if b, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		root = strings.TrimSpace(string(b))
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("working directory: %w", err)
		}
		for d := wd; ; d = filepath.Dir(d) {
			if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
				root = d
				break
			}
			if filepath.Dir(d) == d {
				return "", errors.New("cannot tell where this repository is (no git, no go.mod above the working directory); refusing to write")
			}
		}
	}
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	return real, nil
}

// readCapped reads the export, refusing one larger than the exporter accepts.
func readCapped(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open export: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, evals.MaxExportBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read export: %w", err)
	}
	if len(b) > evals.MaxExportBytes {
		return nil, fmt.Errorf("export is larger than %d bytes", evals.MaxExportBytes)
	}
	return b, nil
}
