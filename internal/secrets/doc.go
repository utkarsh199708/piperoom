// Package secrets reads secret values from the customer's secret managers at
// run time (docs/design/secret-providers.md). Piperoom stores only a
// reference such as secret://vault-corp/database/creds/piperoom-ro#password;
// the value is fetched when a client is built, held in memory as a
// netsec.Secret and never written to the database, a file, a log, an event
// or the browser.
//
// The pieces:
//
//   - Ref and ParseRef: the secret:// reference format (§2).
//   - Provider: one secret manager. Built in here: mounted files, environment
//     variables, HashiCorp Vault / OpenBao, Kubernetes Secrets, AWS Secrets
//     Manager and AWS SSM Parameter Store. Azure and Google live in sub-packages.
//   - Registry: the spec.Resolver every integration uses. It finds the
//     provider of a reference in the caller's workspace only, caches values
//     in memory with a TTL, shares one in-flight fetch between callers,
//     renews Vault leases at two thirds of their TTL and re-reads a value on
//     demand after a login failure (ResolveFresh).
//   - ParseProviders and FromConfig: providers declared in
//     PIPEROOM_SECRET_PROVIDERS.
package secrets
