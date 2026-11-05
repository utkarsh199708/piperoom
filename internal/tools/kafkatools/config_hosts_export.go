package kafkatools

// Exported views of the Q4a host-guard primitives for package actions, whose
// connect_create_connector (Q5c) guards the hosts of a to-be-created config
// with the same reduction rules the validate tool uses. One rule, two
// callers: a key this package flags as host-bearing is exactly what the
// create action refuses to point anywhere the pipeline does not already
// reach.

// IsHostConfigKey reports whether a config key names a host (or host list)
// Connect may be asked to reach. See isHostConfigKey.
func IsHostConfigKey(k string) bool { return isHostConfigKey(k) }

// ConfigHosts reduces a host value, DSN/JDBC URL or host list to its
// normalized host names. See configHosts.
func ConfigHosts(v string) []string { return configHosts(v) }

// AddConfigHosts adds the hosts named by a server-side config's host keys.
// See addConfigHosts.
func AddConfigHosts(set map[string]bool, cfg map[string]string) { addConfigHosts(set, cfg) }

// ValidHost reports whether a reduced entry is a plain host name; an entry
// that is not fails closed in every caller.
func ValidHost(h string) bool { return validHostRe.MatchString(h) }
