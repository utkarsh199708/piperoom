package knowledge

// The image allow-list. Every Digest is the registry manifest digest — a
// multi-arch index digest where the image is multi-arch — fetched with the
// registry's token + manifest flow (Docker Hub and Quay) or directly from the
// registry (MCR) and read from the Docker-Content-Digest response header.
// Source is the manifest URL the digest was read from.
//
// Registry names: "docker.io" (Docker Hub), "quay.io" (Red Hat Quay),
// "mcr.microsoft.com" (Microsoft Container Registry). Repo is the path after
// the registry hostname; Docker official images use the short name and are
// also accepted with the "library/" prefix.
//
// Coverage: one pinned tag per image kind — the versions the Piperoom setup
// templates and the dev stack use. Connect and Server share the Debezium
// 3.2.0.Final release so the plugin digests in plugins.go and the image tag
// agree; Kafka 4.1.0 and Schema Registry 8.0.0 are the KRaft/Confluent pair
// of dev/docker-compose.yml; the five source databases are one pinned image
// each for the U wave sources (Postgres, MySQL, SQL Server, MongoDB, Oracle).
//
// Moving tags: postgres:17 and mssql/server:2022-latest are floating tags the
// registries re-publish, so a refresh may report a mismatch against the live
// tag until the pin here is re-fetched. The pin stays valid for the exact
// manifest the digest was read from.
var images = []Image{
	{
		// Debezium Connect (Kafka Connect with the Debezium connectors).
		// Source: https://quay.io/v2/debezium/connect/manifests/3.2.0.Final
		Name:     "connect",
		Registry: "quay.io",
		Repo:     "debezium/connect",
		Tag:      "3.2.0.Final",
		Digest:   "sha256:b79f70f10a4aa97445f61e1c7b6cc8f484018f59d9a90cabc8135d3ae70ad90b",
		Source:   "https://quay.io/v2/debezium/connect/manifests/3.2.0.Final",
	},
	{
		// Debezium Server (the standalone CDC streamer).
		// Source: https://quay.io/v2/debezium/server/manifests/3.2.0.Final
		Name:     "server",
		Registry: "quay.io",
		Repo:     "debezium/server",
		Tag:      "3.2.0.Final",
		Digest:   "sha256:197f5eaf72b8bfcad1f0a8cca6c663fa1acecde72aa287b7617cef912a218e18",
		Source:   "https://quay.io/v2/debezium/server/manifests/3.2.0.Final",
	},
	{
		// Apache Kafka in KRaft mode (the "kafka-kraft" template).
		// Source: https://registry-1.docker.io/v2/apache/kafka/manifests/4.1.0
		Name:     "kafka",
		Registry: "docker.io",
		Repo:     "apache/kafka",
		Tag:      "4.1.0",
		Digest:   "sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f",
		Source:   "https://registry-1.docker.io/v2/apache/kafka/manifests/4.1.0",
	},
	{
		// Confluent Schema Registry (the "registry" template).
		// Source: https://registry-1.docker.io/v2/confluentinc/cp-schema-registry/manifests/8.0.0
		Name:     "schema-registry",
		Registry: "docker.io",
		Repo:     "confluentinc/cp-schema-registry",
		Tag:      "8.0.0",
		Digest:   "sha256:9c1a706e7a40c624d788c2256af0173c664204b7d03dd7013b4560d41e090143",
		Source:   "https://registry-1.docker.io/v2/confluentinc/cp-schema-registry/manifests/8.0.0",
	},
	{
		// Local Postgres source (the "source-postgres" template).
		// Source: https://registry-1.docker.io/v2/library/postgres/manifests/17
		Name:     "postgres",
		Registry: "docker.io",
		Repo:     "postgres",
		Tag:      "17",
		Digest:   "sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3",
		Source:   "https://registry-1.docker.io/v2/library/postgres/manifests/17",
	},
	{
		// Local MySQL source (U2, MySQL 8.4 LTS).
		// Source: https://registry-1.docker.io/v2/library/mysql/manifests/8.4
		Name:     "mysql",
		Registry: "docker.io",
		Repo:     "mysql",
		Tag:      "8.4",
		Digest:   "sha256:0f51e63e537a1f176423fa03ef559aabe6fa4372a53160a44bc7e75da7d15364",
		Source:   "https://registry-1.docker.io/v2/library/mysql/manifests/8.4",
	},
	{
		// Local MongoDB source (U7).
		// Source: https://registry-1.docker.io/v2/library/mongo/manifests/8.0
		Name:     "mongodb",
		Registry: "docker.io",
		Repo:     "mongo",
		Tag:      "8.0",
		Digest:   "sha256:4a4ab5d4834207389e0d7270fac988e21ffedccecba77991a552f755df229907",
		Source:   "https://registry-1.docker.io/v2/library/mongo/manifests/8.0",
	},
	{
		// Local Oracle Database Free (U9); gvenzl/oracle-free is the
		// widely used Docker Hub distribution of Oracle Database Free.
		// Source: https://registry-1.docker.io/v2/gvenzl/oracle-free/manifests/23-slim
		Name:     "oracle",
		Registry: "docker.io",
		Repo:     "gvenzl/oracle-free",
		Tag:      "23-slim",
		Digest:   "sha256:6d61d267a3b978c24c5ac1790e62e927416a0aec446bd86e4b3a1527562757bd",
		Source:   "https://registry-1.docker.io/v2/gvenzl/oracle-free/manifests/23-slim",
	},
	{
		// Local SQL Server source (U4). SQL Server images are amd64-only, so
		// this is a single-arch manifest, not an index.
		// Source: https://mcr.microsoft.com/v2/mssql/server/manifests/2022-latest
		Name:     "sqlserver",
		Registry: "mcr.microsoft.com",
		Repo:     "mssql/server",
		Tag:      "2022-latest",
		Digest:   "sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718",
		Source:   "https://mcr.microsoft.com/v2/mssql/server/manifests/2022-latest",
	},
}
