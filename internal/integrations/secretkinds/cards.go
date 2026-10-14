package secretkinds

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/secrets"
	"piperoom/internal/secrets/azure"
	"piperoom/internal/secrets/gcp"
)

// Patterns the server checks on save and test, and that the setup scripts
// use to decide whether a value may appear in the script (anything else
// becomes a <placeholder>, so a script never carries pasted shell text).
const (
	httpsURLPattern  = `^https?://[^\s/?#]+/?$`
	awsRegionPattern = `^[a-z]{2}(-[a-z]+)+-[0-9]+$`
	awsAccountPat    = `^[0-9]{12}$`
	kubeNamePattern  = `^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	vaultPathPattern = `^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*/?$`
	vaultNSPattern   = `^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*/?$`
	gcpProjectPat    = `^[a-z][a-z0-9-]{4,28}[a-z0-9]$`
)

var (
	awsRegionRe  = regexp.MustCompile(awsRegionPattern)
	awsAccountRe = regexp.MustCompile(awsAccountPat)
	kubeNameRe   = regexp.MustCompile(kubeNamePattern)
	vaultPathRe  = regexp.MustCompile(vaultPathPattern)
	vaultRoleRe  = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	kmsKeyRe     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

func orPlaceholder(s string, re *regexp.Regexp, placeholder string) string {
	if s = strings.TrimSpace(s); s != "" && re.MatchString(s) {
		return s
	}
	return placeholder
}

func prefixField(def, help string) spec.Field {
	return spec.Field{Name: "prefix", Label: "Path prefix", Help: help, Type: spec.FieldText, Default: def}
}

// --- HashiCorp Vault / OpenBao ---------------------------------------------

func vaultCard() card {
	return card{
		id: KindVault, ptype: secrets.TypeVault, logo: "vault",
		title:      "HashiCorp Vault / OpenBao",
		summary:    "Read secrets from Vault or OpenBao: KV v1/v2 and short-lived database logins",
		samplePath: "secret/piperoom/source-db#password",
		fields: []spec.Field{
			{Name: "address", Label: "Address", Help: "https://vault.corp:8200 (http only to localhost)", Type: spec.FieldText, Required: true, Pattern: httpsURLPattern},
			{Name: "namespace", Label: "Namespace", Help: "Vault Enterprise / HCP namespace; empty = root", Type: spec.FieldText, Pattern: vaultNSPattern},
			{Name: "auth", Label: "Log in with", Type: spec.FieldSelect, Default: secrets.VaultAuthKubernetes, Options: []string{
				secrets.VaultAuthKubernetes, secrets.VaultAuthAppRole, secrets.VaultAuthJWT, secrets.VaultAuthToken,
			}, Help: "Kubernetes or JWT workload identity is preferred; a static token is a last resort"},
			{Name: "mount", Label: "Auth mount", Help: "Empty = the method's name (kubernetes, approle, jwt)", Type: spec.FieldText, Pattern: vaultPathPattern},
			{Name: "role", Label: "Vault role", Help: "kubernetes and jwt: the role bound to Piperoom", Type: spec.FieldText, Pattern: `^[A-Za-z0-9_.-]+$`},
			{Name: "role_id", Label: "AppRole role id", Help: "approle only", Type: spec.FieldText},
			{Name: "secret_id", Label: "AppRole secret id", Help: "approle only", Type: spec.FieldSecret},
			{Name: "jwt", Label: "JWT", Help: "jwt only; empty = the mounted service-account token", Type: spec.FieldSecret},
			{Name: "token", Label: "Token", Help: "token only; prefer a periodic, orphan token with only Piperoom's policy", Type: spec.FieldSecret},
			{Name: "kv2_mounts", Label: "KV v2 mounts", Help: "Comma-separated; other mounts are read as is (KV v1, database)", Type: spec.FieldText, Default: "secret"},
			prefixField("secret/piperoom/", "Piperoom reads under this path; the least-privilege check probes outside it"),
			{Name: "ca_cert", Label: "CA certificate", Help: "PEM; empty = the system's trusted CAs", Type: spec.FieldPEM},
		},
		spec: func(v spec.Values) (secrets.ProviderSpec, string, bool) {
			ps := secrets.ProviderSpec{Address: strings.TrimSpace(v.Get("address")), Options: map[string]string{}, Secrets: secretsOf(v, "secret_id", "jwt", "token")}
			for _, k := range []string{"namespace", "auth", "mount", "role", "role_id", "kv2_mounts", "prefix"} {
				put(ps.Options, v, k)
			}
			return ps, v.Secret("ca_cert").Reveal(), true
		},
		script: vaultScript,
	}
}

func secretsOf(v spec.Values, names ...string) map[string]netsec.Secret {
	out := map[string]netsec.Secret{}
	for _, n := range names {
		if s := v.Secret(n); s != "" {
			out[n] = s
		}
	}
	return out
}

// vaultScript is the policy HCL plus the auth-role binding (§5).
func vaultScript(v spec.Values) string {
	kv2 := map[string]bool{}
	for _, m := range strings.Split(v.Get("kv2_mounts"), ",") {
		if m = strings.Trim(strings.TrimSpace(m), "/"); m != "" {
			kv2[m] = true
		}
	}
	if v.Get("kv2_mounts") == "" {
		kv2["secret"] = true
	}
	prefix := strings.Trim(orPlaceholder(v.Get("prefix"), vaultPathRe, "secret/piperoom"), "/")
	mount, rest, _ := strings.Cut(prefix, "/")
	auth := v.Get("auth")
	if auth == "" {
		auth = secrets.VaultAuthKubernetes
	}
	authMount := strings.Trim(orPlaceholder(v.Get("mount"), vaultPathRe, auth), "/")
	role := orPlaceholder(v.Get("role"), vaultRoleRe, "piperoom")

	var b strings.Builder
	b.WriteString("# Piperoom read-only Vault policy. Save as piperoom-ro.hcl, then run the commands below.\n")
	b.WriteString("# Token lookup/renew and sys/capabilities-self (the least-privilege check) come from Vault's default policy.\n")
	if kv2[mount] {
		b.WriteString("path \"" + mount + "/data/" + rest + "/*\" {\n  capabilities = [\"read\"]\n}\n")
		b.WriteString("path \"" + mount + "/metadata/" + rest + "/*\" {\n  capabilities = [\"list\", \"read\"]\n}\n")
	} else {
		b.WriteString("path \"" + prefix + "/*\" {\n  capabilities = [\"read\", \"list\"]\n}\n")
	}
	b.WriteString("# Short-lived database logins (optional): uncomment and name your read-only role.\n")
	b.WriteString("# path \"database/creds/piperoom-ro\" {\n#   capabilities = [\"read\"]\n# }\n\n")
	b.WriteString("vault policy write piperoom-ro piperoom-ro.hcl\n")
	switch auth {
	case secrets.VaultAuthKubernetes:
		b.WriteString("vault write auth/" + authMount + "/role/" + role +
			" bound_service_account_names=piperoom bound_service_account_namespaces=<piperoom-namespace> token_policies=piperoom-ro token_ttl=1h\n")
	case secrets.VaultAuthAppRole:
		b.WriteString("vault write auth/" + authMount + "/role/" + role + " token_policies=piperoom-ro token_ttl=1h secret_id_ttl=720h\n")
		b.WriteString("vault read auth/" + authMount + "/role/" + role + "/role-id\n")
		b.WriteString("vault write -f auth/" + authMount + "/role/" + role + "/secret-id\n")
	case secrets.VaultAuthJWT:
		b.WriteString("vault write auth/" + authMount + "/role/" + role +
			" role_type=jwt bound_audiences=<audience> user_claim=sub bound_subject=<piperoom-subject> token_policies=piperoom-ro token_ttl=1h\n")
	default:
		b.WriteString("# Last resort: a periodic orphan token with only this policy. Prefer workload identity.\n")
		b.WriteString("vault token create -orphan -policy=piperoom-ro -period=24h\n")
	}
	return b.String()
}

// --- AWS -----------------------------------------------------------------

func awsFields(prefixDef string) []spec.Field {
	return []spec.Field{
		{Name: "region", Label: "Region", Help: "e.g. eu-west-1", Type: spec.FieldText, Required: true, Pattern: awsRegionPattern},
		prefixField(prefixDef, "Piperoom reads names starting with this"),
		{Name: "account_id", Label: "AWS account id", Help: "Only for the setup script", Type: spec.FieldText, Pattern: awsAccountPat},
		{Name: "kms_key_id", Label: "KMS key id", Help: "Only for the setup script; empty = the AWS managed key", Type: spec.FieldText},
	}
}

func awsSpec(v spec.Values) (secrets.ProviderSpec, string, bool) {
	// Credentials come from the server's own identity (instance role, IRSA,
	// ECS task role): no long-lived keys in the form.
	return secrets.ProviderSpec{Address: strings.TrimSpace(v.Get("region")), Options: map[string]string{}}, "", false
}

func awsSMCard() card {
	return card{
		id: KindAWSSM, ptype: secrets.TypeAWSSecretsManager, logo: "aws",
		title:      "AWS Secrets Manager",
		summary:    "Read secrets with the server's AWS identity; no access keys stored",
		samplePath: "piperoom/source-db#password",
		fields:     awsFields("piperoom/"),
		spec:       awsSpec,
		script:     func(v spec.Values) string { return awsScript(v, "secretsmanager") },
	}
}

func awsSSMCard() card {
	return card{
		id: KindAWSSSM, ptype: secrets.TypeAWSSSM, logo: "aws",
		title:      "AWS SSM Parameter Store",
		summary:    "Read SecureString parameters with the server's AWS identity",
		samplePath: "piperoom/source-db/password",
		fields:     awsFields("piperoom/"),
		spec:       awsSpec,
		script:     func(v spec.Values) string { return awsScript(v, "ssm") },
	}
}

type iamStatement struct {
	Sid       string                       `json:"Sid"`
	Effect    string                       `json:"Effect"`
	Action    []string                     `json:"Action"`
	Resource  string                       `json:"Resource"`
	Condition map[string]map[string]string `json:"Condition,omitempty"`
}

// awsScript is the IAM policy JSON for the server's role (§5).
func awsScript(v spec.Values, service string) string {
	region := orPlaceholder(v.Get("region"), awsRegionRe, "<region>")
	account := orPlaceholder(v.Get("account_id"), awsAccountRe, "<account-id>")
	prefix := strings.TrimPrefix(orPlaceholder(v.Get("prefix"), vaultPathRe, "piperoom/"), "/")
	key := "alias/aws/" + service
	if k := orPlaceholder(v.Get("kms_key_id"), kmsKeyRe, ""); k != "" {
		key = "key/" + k
	}
	var st []iamStatement
	if service == "secretsmanager" {
		st = []iamStatement{
			{Sid: "ReadPiperoomSecrets", Effect: "Allow", Action: []string{"secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"},
				Resource: "arn:aws:secretsmanager:" + region + ":" + account + ":secret:" + prefix + "*"},
			{Sid: "ListSecretNamesForPicker", Effect: "Allow", Action: []string{"secretsmanager:ListSecrets"}, Resource: "*"},
		}
	} else {
		st = []iamStatement{
			{Sid: "ReadPiperoomParameters", Effect: "Allow", Action: []string{"ssm:GetParameter", "ssm:GetParameters"},
				Resource: "arn:aws:ssm:" + region + ":" + account + ":parameter/" + prefix + "*"},
			{Sid: "ListParameterNamesForPicker", Effect: "Allow", Action: []string{"ssm:DescribeParameters"}, Resource: "*"},
		}
	}
	st = append(st, iamStatement{
		Sid: "DecryptOnlyViaService", Effect: "Allow", Action: []string{"kms:Decrypt"},
		Resource:  "arn:aws:kms:" + region + ":" + account + ":" + key,
		Condition: map[string]map[string]string{"StringEquals": {"kms:ViaService": service + "." + region + ".amazonaws.com"}},
	})
	doc, _ := json.MarshalIndent(struct {
		Version   string         `json:"Version"`
		Statement []iamStatement `json:"Statement"`
	}{"2012-10-17", st}, "", "  ")
	return "// Attach to the IAM role Piperoom runs as. The List action cannot be scoped by AWS;\n" +
		"// drop it to turn off the name picker (references are then typed by hand).\n" + string(doc) + "\n"
}

// --- Kubernetes ------------------------------------------------------------

func kubeCard() card {
	return card{
		id: KindKubernetes, ptype: secrets.TypeKubernetes, logo: "kubernetes",
		title:      "Kubernetes Secrets",
		summary:    "Read Secrets in one namespace with Piperoom's service account",
		samplePath: "source-db#password",
		fields: []spec.Field{
			{Name: "namespace", Label: "Namespace", Help: "Empty = Piperoom's own namespace", Type: spec.FieldText, Pattern: kubeNamePattern},
			{Name: "address", Label: "API server", Help: "Empty = the cluster Piperoom runs in", Type: spec.FieldText, Pattern: httpsURLPattern},
			{Name: "ca_cert", Label: "CA certificate", Help: "PEM, only with an API server address", Type: spec.FieldPEM},
			{Name: "secret_names", Label: "Secret names", Help: "Only for the setup script: comma-separated Secrets Piperoom may read", Type: spec.FieldText},
			{Name: "service_account", Label: "Service account", Help: "Only for the setup script", Type: spec.FieldText, Default: "piperoom", Pattern: kubeNamePattern},
		},
		spec: func(v spec.Values) (secrets.ProviderSpec, string, bool) {
			ps := secrets.ProviderSpec{Address: strings.TrimSpace(v.Get("address")), Options: map[string]string{}}
			put(ps.Options, v, "namespace")
			// In-cluster (no address) keeps the provider's own client, which
			// trusts the mounted cluster CA; an admin-typed address is guarded.
			return ps, v.Secret("ca_cert").Reveal(), ps.Address != ""
		},
		script: kubeScript,
	}
}

// kubeScript is a Role with get on the named Secrets and its RoleBinding.
func kubeScript(v spec.Values) string {
	ns := orPlaceholder(v.Get("namespace"), kubeNameRe, "<namespace>")
	sa := orPlaceholder(v.Get("service_account"), kubeNameRe, "piperoom")
	var names []string
	for _, n := range strings.Split(v.Get("secret_names"), ",") {
		if n = strings.TrimSpace(n); n != "" && kubeNameRe.MatchString(n) {
			names = append(names, `"`+n+`"`)
		}
	}
	if len(names) == 0 {
		names = []string{`"<secret-name>"`}
	}
	return "# Piperoom may read only these Secrets. Without list, the name picker is unavailable\n" +
		"# and references are typed by hand; that is the least-privilege choice.\n" +
		"apiVersion: rbac.authorization.k8s.io/v1\nkind: Role\nmetadata:\n  name: piperoom-secrets-read\n  namespace: " + ns + "\n" +
		"rules:\n  - apiGroups: [\"\"]\n    resources: [\"secrets\"]\n    resourceNames: [" + strings.Join(names, ", ") + "]\n    verbs: [\"get\"]\n" +
		"---\napiVersion: rbac.authorization.k8s.io/v1\nkind: RoleBinding\nmetadata:\n  name: piperoom-secrets-read\n  namespace: " + ns + "\n" +
		"roleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: Role\n  name: piperoom-secrets-read\n" +
		"subjects:\n  - kind: ServiceAccount\n    name: " + sa + "\n    namespace: <piperoom-namespace>\n"
}

// --- Azure Key Vault -----------------------------------------------------

func azureCard() card {
	return card{
		id: KindAzure, ptype: secrets.TypeAzureKeyVault, logo: "azure",
		title:      "Azure Key Vault",
		summary:    "Read secrets with a managed or workload identity",
		samplePath: "source-db-password",
		fields: []spec.Field{
			{Name: "vault_url", Label: "Vault URL", Help: "https://<vault-name>.vault.azure.net", Type: spec.FieldText, Required: true, Pattern: `^https://[^\s/?#]+/?$`},
			{Name: "auth", Label: "Log in with", Type: spec.FieldSelect, Default: "auto", Options: []string{
				"auto", azure.AuthManagedIdentity, azure.AuthWorkloadIdentity, azure.AuthClientSecret,
			}, Help: "auto picks workload identity when its token file is mounted, else managed identity"},
			{Name: "tenant_id", Label: "Tenant id", Help: "Empty = AZURE_TENANT_ID", Type: spec.FieldText},
			{Name: "client_id", Label: "Client id", Help: "User-assigned identity or app registration; empty = AZURE_CLIENT_ID", Type: spec.FieldText},
			{Name: "client_secret", Label: "Client secret", Help: "client_secret only; a last resort", Type: spec.FieldSecret},
			{Name: "principal_object_id", Label: "Identity object id", Help: "Only for the setup script", Type: spec.FieldText},
		},
		spec: func(v spec.Values) (secrets.ProviderSpec, string, bool) {
			ps := secrets.ProviderSpec{Address: strings.TrimSpace(v.Get("vault_url")), Options: map[string]string{}, Secrets: secretsOf(v, "client_secret")}
			if a := strings.TrimSpace(v.Get("auth")); a != "" && a != "auto" {
				ps.Options["auth"] = a
			}
			put(ps.Options, v, "tenant_id")
			put(ps.Options, v, "client_id")
			return ps, "", true
		},
		script: func(v spec.Values) string {
			name := ""
			if u, err := url.Parse(strings.TrimSpace(v.Get("vault_url"))); err == nil {
				name, _, _ = strings.Cut(u.Hostname(), ".")
			}
			return azure.SetupScript(name, strings.TrimSpace(v.Get("principal_object_id")), nil)
		},
	}
}

// --- Google Secret Manager ---------------------------------------------------

func gcpCard() card {
	return card{
		id: KindGCP, ptype: secrets.TypeGoogleSecretManager, logo: "gcp",
		title:      "Google Secret Manager",
		summary:    "Read secrets with the server's Google service account",
		samplePath: "piperoom-source-db",
		fields: []spec.Field{
			{Name: "project", Label: "Project id", Type: spec.FieldText, Required: true, Pattern: gcpProjectPat},
			prefixField("piperoom-", "Only for the setup script: Piperoom may read secrets whose id starts with this"),
			{Name: "service_account", Label: "Service account email", Help: "Only for the setup script", Type: spec.FieldText},
		},
		spec: func(v spec.Values) (secrets.ProviderSpec, string, bool) {
			// Application default credentials: Google's fixed endpoints, no
			// admin-typed address to guard.
			return secrets.ProviderSpec{Address: strings.TrimSpace(v.Get("project")), Options: map[string]string{}}, "", false
		},
		script: func(v spec.Values) string {
			return gcp.SetupScript(strings.TrimSpace(v.Get("project")), strings.TrimSpace(v.Get("service_account")), strings.TrimSpace(v.Get("prefix")))
		},
	}
}
