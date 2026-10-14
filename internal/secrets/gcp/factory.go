package gcp

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/secrets"
)

// Factory builds a provider from a PIPEROOM_SECRET_PROVIDERS entry:
//
//	gsm-prod=gcp-secret-manager:acme-prod;credentials_file=/var/run/secrets/gcp/sa.json
//
// The address is the project id. Options: endpoint,
// resource_manager_endpoint, credentials_file, ca_file, server_name,
// timeout. Unknown options are an error.
func Factory(_ context.Context, s secrets.ProviderSpec, d secrets.Deps) (secrets.Provider, error) {
	used := map[string]bool{}
	get := func(k string) string { used[k] = true; return s.Options[k] }
	opts := Options{
		Project:                 s.Address,
		Endpoint:                get("endpoint"),
		ResourceManagerEndpoint: get("resource_manager_endpoint"),
		CredentialsFile:         get("credentials_file"),
		TLS:                     netsec.TLS{CAFile: get("ca_file"), ServerName: get("server_name")},
		HTTPClient:              d.HTTPClient,
	}
	if t := get("timeout"); t != "" {
		dur, err := time.ParseDuration(t)
		if err != nil || dur <= 0 {
			return nil, fmt.Errorf("%w: provider %q: option timeout must be a positive duration", secrets.ErrBadProviderConfig, s.ID)
		}
		opts.Timeout = dur
	}
	var extra []string
	for k := range s.Options {
		if !used[k] {
			extra = append(extra, k)
		}
	}
	for k := range s.Secrets {
		extra = append(extra, k)
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return nil, fmt.Errorf("%w: provider %q (%s): unknown options %s", secrets.ErrBadProviderConfig, s.ID, s.Type, strings.Join(extra, ", "))
	}
	p, err := New(opts)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Register adds Factory to m under secrets.TypeGoogleSecretManager.
func Register(m map[string]secrets.Factory) { m[secrets.TypeGoogleSecretManager] = Factory }

var (
	saEmailRe = regexp.MustCompile(`^[a-z][a-z0-9-]{4,61}[a-z0-9]@[a-z0-9.:-]+\.iam\.gserviceaccount\.com$`)
	prefixRe  = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`)
)

// SetupScript returns the least-privilege grant for the card (§5):
// roles/secretmanager.secretAccessor for Piperoom's service account, only
// on secrets whose id starts with prefix (an IAM condition on the project
// binding), plus the optional viewer role for the name picker. Inputs that
// do not match Google's naming rules become placeholders, so the script
// never carries pasted shell text.
func SetupScript(project, serviceAccount, prefix string) string {
	if !projectRe.MatchString(project) {
		project = "<project-id>"
	}
	if !saEmailRe.MatchString(serviceAccount) {
		serviceAccount = "<piperoom-service-account-email>"
	}
	if !prefixRe.MatchString(prefix) {
		prefix = "piperoom-"
	}
	var b strings.Builder
	b.WriteString("# Grant Piperoom read-only access to Secret Manager secrets named " + prefix + "* in " + project + ".\n")
	b.WriteString("PROJECT=" + project + "\n")
	b.WriteString("SA=" + serviceAccount + "\n")
	b.WriteString("PROJECT_NUMBER=$(gcloud projects describe \"$PROJECT\" --format='value(projectNumber)')\n")
	b.WriteString("gcloud projects add-iam-policy-binding \"$PROJECT\" \\\n")
	b.WriteString("  --member \"serviceAccount:$SA\" --role roles/secretmanager.secretAccessor \\\n")
	b.WriteString("  --condition \"title=piperoom-" + strings.Trim(prefix, "-_") + ",expression=resource.name.startsWith('projects/$PROJECT_NUMBER/secrets/" + prefix + "')\"\n")
	b.WriteString("# Optional, for the name picker (names and metadata, never values):\n")
	b.WriteString("# gcloud projects add-iam-policy-binding \"$PROJECT\" --member \"serviceAccount:$SA\" --role roles/secretmanager.viewer\n")
	b.WriteString("# Or, per secret instead of the condition:\n")
	b.WriteString("# gcloud secrets add-iam-policy-binding <secret> --project \"$PROJECT\" --member \"serviceAccount:$SA\" --role roles/secretmanager.secretAccessor\n")
	return b.String()
}
