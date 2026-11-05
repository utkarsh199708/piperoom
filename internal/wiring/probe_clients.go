package wiring

import (
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// The helpers below let internal/connprobe build a throwaway client from the
// same settings mapping Open uses, so a connection test connects the way the
// server will. They do no I/O.

// KafkaSecurity maps the Kafka security settings of cfg onto netsec as Open
// does. A probe has no shared AWS config, so with KAFKA_SASL_MECHANISM=
// AWS_MSK_IAM it returns an error; check UsesMSKIAM first.
func KafkaSecurity(cfg config.Config) (netsec.Kafka, error) {
	return kafkaSecurity(cfg, nil)
}

// UsesMSKIAM reports whether cfg selects the AWS_MSK_IAM SASL mechanism.
func UsesMSKIAM(cfg config.Config) bool { return usesMSKIAM(cfg) }

// SplitBrokers splits KAFKA_BROKERS as Open does: comma separated, blanks
// trimmed, empty entries dropped.
func SplitBrokers(s string) []string { return splitBrokers(s) }

// ConnectHTTP returns the Kafka Connect login and TLS settings of cfg, the
// same fields buildA2Clients hands to connect.NewSecure.
func ConnectHTTP(cfg config.Config) netsec.HTTP {
	return netsec.HTTP{
		Username: cfg.ConnectUsername,
		Password: netsec.Secret(cfg.ConnectPassword),
		TLS: netsec.TLS{
			CAFile:   cfg.ConnectTLSCAFile,
			CertFile: cfg.ConnectTLSCertFile,
			KeyFile:  cfg.ConnectTLSKeyFile,
		},
	}
}

// RegistryHTTP returns the Schema Registry login and TLS settings of cfg, the
// same fields buildA2Clients hands to schemareg.NewSecure.
func RegistryHTTP(cfg config.Config) netsec.HTTP {
	return netsec.HTTP{
		Username: cfg.SchemaRegistryUsername,
		Password: netsec.Secret(cfg.SchemaRegistryPassword),
		TLS: netsec.TLS{
			CAFile:   cfg.SchemaRegistryTLSCAFile,
			CertFile: cfg.SchemaRegistryTLSCertFile,
			KeyFile:  cfg.SchemaRegistryTLSKeyFile,
		},
	}
}
