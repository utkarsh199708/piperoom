// Command roomcase turns one closed, labelled room into a pending eval scenario.
//
//	go run ./evals/cmd/roomcase -export room.json -owners owners.yaml [-force] [-out DIR]
//
// room.json is a GET /api/rooms/{id}/export document; its integrity hash is
// verified first. Nothing else is read: no database, no network, so this runs
// on any laptop. The scenario lands in DIR/pending/ (default
// evals/scenarios/learned), which evalrun never replays; a person reviews it
// and moves it into DIR in a pull request (evals/README.md, "Learned scenarios").
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

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
	out := fs.String("out", "evals/scenarios/learned", "learned scenarios directory; the file goes to its pending/ subdirectory")
	force := fs.Bool("force", false, "allow a fingerprint that already exists and overwrite the output file (the per-pipeline cap stays)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *export == "" || *own == "" {
		return errors.New("-export and -owners are required")
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
