package azure

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
//	kv-prod=azure-key-vault:https://acme-prod.vault.azure.net;auth=workload_identity;tenant_id=...;client_id=...
//
// Options: auth, tenant_id, client_id, client_secret (held as a secret),
// client_secret_file, federated_token_file, authority_host, resource,
// ca_file, server_name, timeout. Unknown options are an error.
func Factory(_ context.Context, s secrets.ProviderSpec, d secrets.Deps) (secrets.Provider, error) {
	used := map[string]bool{}
	get := func(k string) string { used[k] = true; return s.Options[k] }
	opts := Options{
		VaultURL:           s.Address,
		Auth:               get("auth"),
		TenantID:           get("tenant_id"),
		ClientID:           get("client_id"),
		ClientSecretFile:   get("client_secret_file"),
		FederatedTokenFile: get("federated_token_file"),
		AuthorityHost:      get("authority_host"),
		Resource:           get("resource"),
		TLS:                netsec.TLS{CAFile: get("ca_file"), ServerName: get("server_name")},
		HTTPClient:         d.HTTPClient,
		Getenv:             d.Getenv,
	}
	used["client_secret"] = true
	opts.ClientSecret = s.Secrets["client_secret"]
	if t := get("timeout"); t != "" {
		dur, err := time.ParseDuration(t)
		if err != nil || dur <= 0 {
			return nil, fmt.Errorf("%w: provider %q: option timeout must be a positive duration", secrets.ErrBadProviderConfig, s.ID)
		}
		opts.Timeout = dur
	}
	if err := unknownOptions(s, used); err != nil {
		return nil, err
	}
	p, err := New(opts)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Register adds Factory to m under secrets.TypeAzureKeyVault.
func Register(m map[string]secrets.Factory) { m[secrets.TypeAzureKeyVault] = Factory }

func unknownOptions(s secrets.ProviderSpec, used map[string]bool) error {
	var extra []string
	for k := range s.Options {
		if !used[k] {
			extra = append(extra, k)
		}
	}
	for k := range s.Secrets {
		if !used[k] {
			extra = append(extra, k)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	sort.Strings(extra)
	return fmt.Errorf("%w: provider %q (%s): unknown options %s", secrets.ErrBadProviderConfig, s.ID, s.Type, strings.Join(extra, ", "))
}

var (
	vaultNameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]{1,22}[a-zA-Z0-9]$`)
	guidRe      = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// SetupScript returns the least-privilege grant for the card (§5): the
// built-in "Key Vault Secrets User" role (read values and list names, no
// writes) for Piperoom's identity, on the named secrets when given, else
// on the whole vault. Inputs that do not match Azure's naming rules are
// replaced by placeholders, so the script never carries pasted shell text.
func SetupScript(vaultName, principalObjectID string, secretNames []string) string {
	if !vaultNameRe.MatchString(vaultName) {
		vaultName = "<vault-name>"
	}
	if !guidRe.MatchString(principalObjectID) {
		principalObjectID = "<piperoom-identity-object-id>"
	}
	var b strings.Builder
	b.WriteString("# Grant Piperoom read-only access to Azure Key Vault " + vaultName + ".\n")
	b.WriteString("# Needs the Azure RBAC permission model on the vault (az keyvault update --enable-rbac-authorization true).\n")
	b.WriteString("# \"Key Vault Secrets User\" reads secret values and lists names; it cannot create, change or delete.\n")
	b.WriteString("VAULT_ID=$(az keyvault show --name " + vaultName + " --query id -o tsv)\n")
	assign := "az role assignment create --role \"Key Vault Secrets User\" --assignee-object-id " +
		principalObjectID + " --assignee-principal-type ServicePrincipal --scope "
	var names []string
	for _, n := range secretNames {
		if secretNameRe.MatchString(n) {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		b.WriteString(assign + "\"$VAULT_ID\"\n")
		b.WriteString("# Narrower: assign the role per secret with --scope \"$VAULT_ID/secrets/<secret-name>\" instead.\n")
		b.WriteString("# Azure roles cannot be scoped to a name prefix.\n")
		return b.String()
	}
	b.WriteString("# One assignment per secret. Listing names needs a vault-level grant, so with these\n")
	b.WriteString("# the name picker is unavailable and references are typed by hand.\n")
	for _, n := range names {
		b.WriteString(assign + "\"$VAULT_ID/secrets/" + n + "\"\n")
	}
	return b.String()
}
