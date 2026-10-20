package knowledge

// The Debezium connector plugin digests, per release. Every Digest is the
// digest of the connector's -plugin.tar.gz archive (the unit plugin_install
// downloads and unpacks into the Connect plugin directory), fetched from
// Maven Central. Source is the exact .sha512 or .sha1 URL the digest was read
// from.
//
// Coverage: four release lines — 3.2.0.Final (the image tag in images.go and
// the dev stack), 3.1.3.Final (the last 3.1.x), 3.0.7.Final, and 2.7.2.Final
// (the last 2.x line before 3.0) — each for the five source connectors the U
// wave supports: postgres, mysql, sqlserver, mongodb, oracle. Maven Central
// publishes SHA-512 only for artifacts released from Debezium 3.1 onwards;
// the older releases here are pinned by SHA-1, the strongest digest Maven
// Central serves for them. The release set is a fixed allow-list, not the
// full catalogue: a release absent here is deliberately not offerable.
var plugins = []Plugin{
	// Debezium 3.2.0.Final — SHA-512 from Maven Central.
	{
		Release:   "3.2.0.Final",
		Connector: "postgres",
		Artifact:  "debezium-connector-postgres",
		Digest:    "sha512:0f1b1d505c1b1f5ad12746de8f4150d6eb4c5d8c1930e22b189a341e59d22c8b0edd189b008fd2e9275e9b3681170de390466202a88894754622997764cf020b",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.2.0.Final/debezium-connector-postgres-3.2.0.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.2.0.Final",
		Connector: "mysql",
		Artifact:  "debezium-connector-mysql",
		Digest:    "sha512:dab9ea56a1ad3bffa4718dfb4f55823d746770dac60b0a0d03922efb7e0c118f774b5c7b0c77d9dd06a6805f5505c9c6732416844b3579743037ce897ae4aa68",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.2.0.Final/debezium-connector-mysql-3.2.0.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.2.0.Final",
		Connector: "sqlserver",
		Artifact:  "debezium-connector-sqlserver",
		Digest:    "sha512:090c109f238d0941b452f66528c807e335c31089da82b16718248a3ef43081a46488cb3b2bfb4b6eedaf03348a0b4de42474ded8b5e0beb4f6ed599212cdc3a7",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/3.2.0.Final/debezium-connector-sqlserver-3.2.0.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.2.0.Final",
		Connector: "mongodb",
		Artifact:  "debezium-connector-mongodb",
		Digest:    "sha512:d81878cf58f179ee86ddf2698c9d6f22089b3359b537d1813fa090886fe0ba53e17a0bd47a545cb6e837e745c45a4b6ad4cabcf91203de624c8b0929d875dfc5",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/3.2.0.Final/debezium-connector-mongodb-3.2.0.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.2.0.Final",
		Connector: "oracle",
		Artifact:  "debezium-connector-oracle",
		Digest:    "sha512:ae55a0e07858c400f0d1a3b08a5e63a143d1df33a7e1bdaebeb70bd2e37c90277cc0be529c5c34c43b69c3c98273a791323f98ab5a84f4b9a5acafa7d6a3e4f8",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/3.2.0.Final/debezium-connector-oracle-3.2.0.Final-plugin.tar.gz.sha512",
	},

	// Debezium 3.1.3.Final — SHA-512 from Maven Central.
	{
		Release:   "3.1.3.Final",
		Connector: "postgres",
		Artifact:  "debezium-connector-postgres",
		Digest:    "sha512:5312b8c0dd261e4b0d612fcbf02c83c59dfcc8556919fc5a5a2d407856cbaa1e87c8350efbc20fac387df34c6b55f7917ad07e42c2f991191349b1c03ab4fb5d",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.1.3.Final/debezium-connector-postgres-3.1.3.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.1.3.Final",
		Connector: "mysql",
		Artifact:  "debezium-connector-mysql",
		Digest:    "sha512:833e036ba758eecef000b733ed6bb1f204632560183b193945b0243f95c715952105b699f5c4ead935a206196786f4305bdf259b9e01f2630eb1e0baa9f51ee8",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.1.3.Final/debezium-connector-mysql-3.1.3.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.1.3.Final",
		Connector: "sqlserver",
		Artifact:  "debezium-connector-sqlserver",
		Digest:    "sha512:a8347951dd99e9580f9ff15aa3cb5bc6b207d54d0aafe4dcbcda947f2c07fe51f3864a9b761ac81ff6c580c411b0b456352a1b03e5c489ad2a458c921e1d3098",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/3.1.3.Final/debezium-connector-sqlserver-3.1.3.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.1.3.Final",
		Connector: "mongodb",
		Artifact:  "debezium-connector-mongodb",
		Digest:    "sha512:a2f5026dda23a73eba8a5fcc0acec131a6b949d9474ed8179cf6a44c362d12773afd2a39908ecccef9316f8c23619dcdcca122b96437a6ed0852aac53410e7f6",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/3.1.3.Final/debezium-connector-mongodb-3.1.3.Final-plugin.tar.gz.sha512",
	},
	{
		Release:   "3.1.3.Final",
		Connector: "oracle",
		Artifact:  "debezium-connector-oracle",
		Digest:    "sha512:db0c7165f632efab945881bcf28979aabc148568c57e87131d6a08b09d3706916f3e508e9a66fe3c61bbe1c1755f38cb2c8916f57b117b2a2f963268849dc53d",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/3.1.3.Final/debezium-connector-oracle-3.1.3.Final-plugin.tar.gz.sha512",
	},

	// Debezium 3.0.7.Final — Maven Central serves only SHA-1 for 3.0.x.
	{
		Release:   "3.0.7.Final",
		Connector: "postgres",
		Artifact:  "debezium-connector-postgres",
		Digest:    "sha1:f441be4331e281ebc554f194d90fafe7ba82086e",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.0.7.Final/debezium-connector-postgres-3.0.7.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "mysql",
		Artifact:  "debezium-connector-mysql",
		Digest:    "sha1:31fa51332fe6b32bad0d049011153b054ad893c2",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.0.7.Final/debezium-connector-mysql-3.0.7.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "sqlserver",
		Artifact:  "debezium-connector-sqlserver",
		Digest:    "sha1:f4442fbf6922b301d1bd759d8349e34d4fcdce50",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/3.0.7.Final/debezium-connector-sqlserver-3.0.7.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "mongodb",
		Artifact:  "debezium-connector-mongodb",
		Digest:    "sha1:32954abfea51508e74de37a167f91b81a942f9f2",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/3.0.7.Final/debezium-connector-mongodb-3.0.7.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "oracle",
		Artifact:  "debezium-connector-oracle",
		Digest:    "sha1:dfc708359e3a891a89b3adb20e244de44d60e1a8",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/3.0.7.Final/debezium-connector-oracle-3.0.7.Final-plugin.tar.gz.sha1",
	},

	// Debezium 2.7.2.Final — Maven Central serves only SHA-1 for 2.7.x.
	{
		Release:   "2.7.2.Final",
		Connector: "postgres",
		Artifact:  "debezium-connector-postgres",
		Digest:    "sha1:6dcd05845dc1873654ce24469c7ccb73acd0fb6c",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.2.Final/debezium-connector-postgres-2.7.2.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "mysql",
		Artifact:  "debezium-connector-mysql",
		Digest:    "sha1:f15977a3e99dca4845c0a6766fa8ab1f1403f42b",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.7.2.Final/debezium-connector-mysql-2.7.2.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "sqlserver",
		Artifact:  "debezium-connector-sqlserver",
		Digest:    "sha1:17dd7eecf2968348d1c9e4fb43a12d79d506ef17",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/2.7.2.Final/debezium-connector-sqlserver-2.7.2.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "mongodb",
		Artifact:  "debezium-connector-mongodb",
		Digest:    "sha1:e4d8b1b1e22a4688e7af8fba3a4ea40a3ef4f7a7",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/2.7.2.Final/debezium-connector-mongodb-2.7.2.Final-plugin.tar.gz.sha1",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "oracle",
		Artifact:  "debezium-connector-oracle",
		Digest:    "sha1:4e033b9988e0ad9079e29f39df5f529738fcd31f",
		Source:    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/2.7.2.Final/debezium-connector-oracle-2.7.2.Final-plugin.tar.gz.sha1",
	},
}
