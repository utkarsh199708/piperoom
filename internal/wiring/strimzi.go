package wiring

import (
	"fmt"
	"strings"

	"piperoom/internal/clients/kubernetes"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/k8stools"
)

// The Strimzi incident tools (internal/tools/k8stools) read a pipeline's
// KafkaConnector, KafkaConnect, operator, pods and events through the
// Kubernetes API. Their settings are the pipeline-only keys of its
// connections: block (owners.PipelineOnlyKeys): there is no process-wide
// variable, so only a set opened for one pipeline with its own block can
// have them. Without kubernetes_api_url and strimzi_namespace nothing is
// built and no tool is registered, so every other tool list is unchanged.

// strimziSet is the Kubernetes reader of one client set. The zero value is
// "not configured".
type strimziSet struct {
	kube     k8stools.Reader
	settings k8stools.Settings
}

func (s strimziSet) on() bool { return s.kube != nil }

// kubeConfig is a pipeline's resolved Kubernetes keys.
type kubeConfig struct {
	apiURL    string
	token     netsec.Secret // from env:
	tokenFile string        // from file:, re-read per request
	caFile    string
	settings  k8stools.Settings
}

// pipelineKube resolves the pipeline's Kubernetes keys. ok is false when the
// block sets none (owners.Load refuses a partial set). Errors name the
// pipeline, the key and the reference, never a value.
func pipelineKube(p owners.Pipeline, src connSource) (kubeConfig, bool, error) {
	c := p.Connections
	if _, ok := c["kubernetes_api_url"]; !ok {
		return kubeConfig{}, false, nil
	}
	var kc kubeConfig
	get := func(key string) (string, error) {
		r, ok := c[key]
		if !ok {
			return "", nil
		}
		v, err := resolveRef(key, r, src)
		if err != nil {
			return "", fmt.Errorf("wiring: pipeline %q: connections.%s: %w", p.ID, key, err)
		}
		return strings.TrimSpace(v), nil
	}
	bad := func(key, want string) error {
		return fmt.Errorf("wiring: pipeline %q: connections.%s: the value from %s is not %s (value not shown): %w", p.ID, key, refName(c[key]), want, errConnRef)
	}
	var err error
	if kc.apiURL, err = get("kubernetes_api_url"); err != nil {
		return kubeConfig{}, false, err
	}
	if !kubernetes.ValidBaseURL(kc.apiURL) {
		return kubeConfig{}, false, bad("kubernetes_api_url", "an https://host[:port] URL")
	}
	// The token: env: holds it; file: names a token file the client reads
	// before every request (a projected ServiceAccount token rotates), so
	// only its presence is checked here.
	switch r := c["kubernetes_token"]; {
	case r.File != "":
		if err := src.canRead(r.File); err != nil {
			return kubeConfig{}, false, fmt.Errorf("wiring: pipeline %q: connections.kubernetes_token: open file %s: %w: %w", p.ID, r.File, errConnRef, err)
		}
		kc.tokenFile = r.File
	default:
		tok, err := get("kubernetes_token")
		if err != nil {
			return kubeConfig{}, false, err
		}
		kc.token = netsec.Secret(tok)
	}
	if kc.caFile, err = get("kubernetes_ca_file"); err != nil {
		return kubeConfig{}, false, err
	}
	if kc.settings.Namespace, err = get("strimzi_namespace"); err != nil {
		return kubeConfig{}, false, err
	}
	if !kubernetes.ValidNamespace(kc.settings.Namespace) {
		return kubeConfig{}, false, bad("strimzi_namespace", "a namespace (a DNS-1123 label)")
	}
	if kc.settings.OperatorDeployment, err = get("strimzi_operator_deployment"); err != nil {
		return kubeConfig{}, false, err
	}
	if d := kc.settings.OperatorDeployment; d != "" && !kubernetes.ValidName(d) {
		return kubeConfig{}, false, bad("strimzi_operator_deployment", "a Deployment name (a DNS-1123 subdomain)")
	}
	if kc.settings.OperatorNamespace, err = get("strimzi_operator_namespace"); err != nil {
		return kubeConfig{}, false, err
	}
	if n := kc.settings.OperatorNamespace; n != "" && !kubernetes.ValidNamespace(n) {
		return kubeConfig{}, false, bad("strimzi_operator_namespace", "a namespace (a DNS-1123 label)")
	}
	return kc, true, nil
}

// openStrimzi builds the Kubernetes client of kc. It reads the CA file and
// sends no request.
func openStrimzi(pipelineID string, kc kubeConfig) (strimziSet, error) {
	c, err := kubernetes.New(kubernetes.Config{BaseURL: kc.apiURL, Token: kc.token, TokenFile: kc.tokenFile, CAFile: kc.caFile})
	if err != nil {
		return strimziSet{}, fmt.Errorf("wiring: pipeline %q: kubernetes client: %w", pipelineID, err)
	}
	return strimziSet{kube: c, settings: kc.settings}, nil
}

// registerStrimziTools registers the k8stools over the set's Kubernetes
// reader; nothing without one.
func registerStrimziTools(reg *tools.Registry, w *Wired) error {
	if !w.strimzi.on() {
		return nil
	}
	return k8stools.Register(reg, k8stools.Deps{Kube: w.strimzi.kube, Settings: w.strimzi.settings, Owners: w.Owners})
}
