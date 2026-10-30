package setupcmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"piperoom/internal/setup/targets/strimzi"
)

// inClusterJob is what the Strimzi target's Job (deploy/helm/piperoom,
// setup.runner) needs beyond a terminal runner: the Kubernetes API with the
// Job's projected token, the Secret-based confirmation, and the Strimzi
// driver's options for the registrar.
type inClusterJob struct {
	confirmer *strimzi.SecretConfirmer
	strimzi   strimzi.Options
}

// newInClusterJob builds the Job's API client and confirmer. prefix is
// "piperoom-setup-<room>-confirm"; the pairing Secret the Job read its code
// from is "piperoom-setup-<room>-pairing", deleted once the Welcome arrives.
func newInClusterJob(prefix, pipeline, connectors string) (*inClusterJob, error) {
	room, ok := strings.CutSuffix(prefix, "-confirm")
	if !ok || !strings.HasPrefix(room, "piperoom-setup-") {
		return nil, fmt.Errorf("setup: --confirm-secret-prefix %q is not piperoom-setup-<room>-confirm", prefix)
	}
	cfg, err := strimzi.InClusterConfig(os.Getenv)
	if err != nil {
		return nil, err
	}
	api, err := strimzi.NewAPI(cfg)
	if err != nil {
		return nil, err
	}
	ns, err := strimzi.InClusterNamespace()
	if err != nil {
		return nil, err
	}
	var names []string
	for _, c := range strings.Split(connectors, ",") {
		if c = strings.TrimSpace(c); c != "" {
			names = append(names, c)
		}
	}
	return &inClusterJob{
		confirmer: &strimzi.SecretConfirmer{API: api, Namespace: ns, Prefix: prefix, Pairing: room + "-pairing"},
		strimzi:   strimzi.Options{Pipeline: pipeline, Connectors: names, API: api},
	}, nil
}

// maxCodeFile bounds the pairing code file: a code is a few characters, so
// anything larger is not a code file.
const maxCodeFile = 256

// readCodeFile reads the pairing code from the Job's mounted Secret once.
func readCodeFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("setup: opening the code file: %w", err)
	}
	defer f.Close()
	buf := make([]byte, maxCodeFile+1)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return "", fmt.Errorf("setup: reading the code file: %w", err)
	}
	if n > maxCodeFile {
		return "", errors.New("setup: the code file is larger than a pairing code")
	}
	code := strings.TrimSpace(string(buf[:n]))
	if code == "" {
		return "", errors.New("setup: the code file is empty")
	}
	return code, nil
}
