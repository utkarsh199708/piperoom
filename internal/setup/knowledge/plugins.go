package knowledge

// The Debezium connector plugin digests, per release. Every Digest is the
// SHA-512 of the connector's -plugin.tar.gz archive (the unit plugin_install
// downloads and unpacks into the Connect plugin directory). Where Maven
// Central publishes the .sha512 file (Debezium 3.1 onwards) the digest is
// read from it and Source is that URL; for the older releases Maven Central
// serves no .sha512, so the digest is the SHA-512 computed over the archive
// bytes and Source is the archive URL, with the .sha1 kept as a cross-check
// in each entry's comment.
//
// Coverage: four release lines — 3.2.0.Final (the image tag in images.go and
// the dev stack), 3.1.3.Final (the last 3.1.x), 3.0.7.Final, and 2.7.2.Final
// (the last 2.x line before 3.0) — each for the five source connectors the U
// wave supports: postgres, mysql, sqlserver, mongodb, oracle. The release set
// is a fixed allow-list, not the full catalogue: a release absent here is
// deliberately not offerable.
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

	// Debezium 3.0.7.Final — Maven Central serves no .sha512 for 3.0.x, so
	// each digest is the SHA-512 computed over the -plugin.tar.gz bytes
	// (Source is the archive URL); the .sha1 is kept as a cross-check.
	{
		Release:   "3.0.7.Final",
		Connector: "postgres",
		Artifact:  "debezium-connector-postgres",
		// .sha1 cross-check: f441be4331e281ebc554f194d90fafe7ba82086e
		Digest: "sha512:d7d353c759feb4807172984e2a03440def80b52a452d2d35b72b19f2bd553bde097e0232f7cda0832ca42aa74c868e98a4cc1a9a267fbd4e491a9a2b9bde02ce",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.0.7.Final/debezium-connector-postgres-3.0.7.Final-plugin.tar.gz",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "mysql",
		Artifact:  "debezium-connector-mysql",
		// .sha1 cross-check: 31fa51332fe6b32bad0d049011153b054ad893c2
		Digest: "sha512:569d7f511b9438dcbc6114576470a68e6b32ca6b15773965a2d6fa249cd5f03b00de53beacc2a091493dbed37378e787306da3c48730fa929bf7a0728822b3e7",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.0.7.Final/debezium-connector-mysql-3.0.7.Final-plugin.tar.gz",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "sqlserver",
		Artifact:  "debezium-connector-sqlserver",
		// .sha1 cross-check: f4442fbf6922b301d1bd759d8349e34d4fcdce50
		Digest: "sha512:2b56c158bd177eb4ec4f552302946b1c1c32a62d47ee53502571982a65143326322df58822524fa7ee2d061da7f25e7c0a37c0569ff213754ac5c2e1289c3979",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/3.0.7.Final/debezium-connector-sqlserver-3.0.7.Final-plugin.tar.gz",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "mongodb",
		Artifact:  "debezium-connector-mongodb",
		// .sha1 cross-check: 32954abfea51508e74de37a167f91b81a942f9f2
		Digest: "sha512:9bfdc3b20c7a4a5a31b300e2380ce823994467bf11bed40202913ae177f982f057df6dd14783ffcecc7ef629c44117fd8afaa67d83deb78739cc7372be7bdc3e",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/3.0.7.Final/debezium-connector-mongodb-3.0.7.Final-plugin.tar.gz",
	},
	{
		Release:   "3.0.7.Final",
		Connector: "oracle",
		Artifact:  "debezium-connector-oracle",
		// .sha1 cross-check: dfc708359e3a891a89b3adb20e244de44d60e1a8
		Digest: "sha512:6ca873c143607afc1f7dbb05c4fbde5c452461bd1a15761039fd383e44125d01cf7e9274f17934c44091437875fdf6c5936fc8910bda27f22cf3a2bb0856ac5c",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/3.0.7.Final/debezium-connector-oracle-3.0.7.Final-plugin.tar.gz",
	},

	// Debezium 2.7.2.Final — Maven Central serves no .sha512 for 2.7.x, so
	// each digest is the SHA-512 computed over the -plugin.tar.gz bytes
	// (Source is the archive URL); the .sha1 is kept as a cross-check.
	{
		Release:   "2.7.2.Final",
		Connector: "postgres",
		Artifact:  "debezium-connector-postgres",
		// .sha1 cross-check: 6dcd05845dc1873654ce24469c7ccb73acd0fb6c
		Digest: "sha512:8d208bb24e309c3379578c93cca303f075f0c3466320f1a2e402601b75489bec563e327617829c2874da00c3ba2440656e75af5dde94ed7e79170db05d9365bb",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.2.Final/debezium-connector-postgres-2.7.2.Final-plugin.tar.gz",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "mysql",
		Artifact:  "debezium-connector-mysql",
		// .sha1 cross-check: f15977a3e99dca4845c0a6766fa8ab1f1403f42b
		Digest: "sha512:3c2350f01e3a8062ebcdadb03bc3e6c63017d304f4d02193b3b19c971c9d53f71b4b5aef6a27516da766b18d9800368e23d128e9a54a86864630e00259c49573",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.7.2.Final/debezium-connector-mysql-2.7.2.Final-plugin.tar.gz",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "sqlserver",
		Artifact:  "debezium-connector-sqlserver",
		// .sha1 cross-check: 17dd7eecf2968348d1c9e4fb43a12d79d506ef17
		Digest: "sha512:37ac33973d4433d880ec825eeb5f49b228bdd560b9bb2562a1295551fe501d4419e73e60e89fdc712f58016138d03578fefad7c0d5b39dabbb507a2acb02d503",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/2.7.2.Final/debezium-connector-sqlserver-2.7.2.Final-plugin.tar.gz",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "mongodb",
		Artifact:  "debezium-connector-mongodb",
		// .sha1 cross-check: e4d8b1b1e22a4688e7af8fba3a4ea40a3ef4f7a7
		Digest: "sha512:5960aac000198472760a25eb1ba875d7a7b578f5c24d0843ccfde03a2cc572e2dd852d5ea46672f5461dc21ac2ddd35cd77e265f2d47fe82757ce4fd26c1cfe5",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/2.7.2.Final/debezium-connector-mongodb-2.7.2.Final-plugin.tar.gz",
	},
	{
		Release:   "2.7.2.Final",
		Connector: "oracle",
		Artifact:  "debezium-connector-oracle",
		// .sha1 cross-check: 4e033b9988e0ad9079e29f39df5f529738fcd31f
		Digest: "sha512:de2759c61bd8219526f2e31ccfc5a4b7bda557a5136a29eca883fe98f943fb0f40492d15fd91bd8ad27e27a3ac9394810319e3afb5ea59fd130df32f0257c44e",
		Source: "https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/2.7.2.Final/debezium-connector-oracle-2.7.2.Final-plugin.tar.gz",
	},
}
