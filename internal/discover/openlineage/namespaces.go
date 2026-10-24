package openlineage

import (
	"net"
	"net/url"
	"strings"

	"piperoom/internal/config"
	"piperoom/internal/topology"
)

// Namespaces are the OpenLineage dataset namespaces of the process-wide
// connections (docs/design/lineage.md section 6.2), and the database part of
// the dataset names. They hold host, port and database names only: user,
// password and query parameters of a DSN never reach them. An empty
// namespace leaves that kind of node out of every event.
type Namespaces struct {
	Source        string // "postgres://host:port" or "mysql://host:port" of SOURCE_DB_URL
	SourceDB      string // its database
	Kafka         string // "kafka://host:port" of the first KAFKA_BROKERS entry
	WarehouseKind string // WAREHOUSE_KIND, lower case; "" = postgres
	Warehouse     string // the warehouse namespace
	// WarehouseDB is the first part of a sink table's dataset name: the
	// Postgres or Snowflake database, the BigQuery project, the Databricks
	// catalog, the ClickHouse database (used when the table has none).
	WarehouseDB string
}

// NamespacesFromConfig derives the namespaces from the process-wide
// settings. A value it cannot read leaves its namespace empty; nothing it
// returns quotes the DSN.
func NamespacesFromConfig(c config.Config) Namespaces {
	n := Namespaces{WarehouseKind: strings.ToLower(strings.TrimSpace(c.WarehouseKind))}
	if n.WarehouseKind == "" {
		n.WarehouseKind = "postgres"
	}
	n.Source, n.SourceDB = postgresNamespace(c.SourceDBURL)
	if n.Source == "" {
		n.Source, n.SourceDB = mysqlNamespace(c.SourceDBURL)
	}
	if first, _, _ := strings.Cut(c.KafkaBrokers, ","); strings.TrimSpace(first) != "" {
		if hp := hostPort(strings.TrimSpace(first), "9092"); hp != "" {
			n.Kafka = "kafka://" + hp
		}
	}
	dsn := strings.TrimSpace(c.WarehouseDBURL)
	switch n.WarehouseKind {
	case "postgres":
		n.Warehouse, n.WarehouseDB = postgresNamespace(dsn)
	case "clickhouse":
		if u, err := url.Parse(dsn); err == nil && u.Host != "" {
			if hp := hostPort(u.Host, "9000"); hp != "" {
				n.Warehouse, n.WarehouseDB = "clickhouse://"+hp, strings.Trim(u.Path, "/")
			}
		}
	case "bigquery":
		// bigquery://<project>/<location>: the namespace is the constant
		// "bigquery", the project starts the name.
		if u, err := url.Parse(dsn); err == nil && u.Host != "" && u.User == nil {
			n.Warehouse, n.WarehouseDB = "bigquery", u.Host
		}
	case "databricks":
		// [https://][token:<pat>@]<host>[:<port>]/<http-path>?catalog=<c>
		raw := dsn
		if !strings.Contains(raw, "://") {
			raw = "https://" + raw
		}
		if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
			if cat := u.Query().Get("catalog"); cat != "" {
				n.Warehouse, n.WarehouseDB = "unitycatalog://"+strings.ToLower(u.Hostname()), cat
			}
		}
	case "snowflake":
		n.Warehouse, n.WarehouseDB = snowflakeNamespace(dsn)
	}
	return n
}

// postgresNamespace reads a Postgres URL (postgres:// or postgresql://) or
// keyword/value DSN ("host=... port=... dbname=...").
func postgresNamespace(dsn string) (ns, db string) {
	dsn = strings.TrimSpace(dsn)
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil || u.Host == "" || strings.Contains(u.Host, ",") {
			return "", ""
		}
		db = strings.Trim(u.Path, "/")
		if hp := hostPort(u.Host, "5432"); hp != "" && db != "" {
			return "postgres://" + hp, db
		}
		return "", ""
	}
	kv := map[string]string{}
	for _, f := range strings.Fields(dsn) {
		if k, v, ok := strings.Cut(f, "="); ok {
			kv[k] = strings.Trim(v, "'")
		}
	}
	port := kv["port"]
	if port == "" {
		port = "5432"
	}
	if kv["host"] == "" || kv["dbname"] == "" || strings.Contains(kv["host"], ",") {
		return "", ""
	}
	if hp := hostPort(net.JoinHostPort(kv["host"], port), "5432"); hp != "" {
		return "postgres://" + hp, kv["dbname"]
	}
	return "", ""
}

// mysqlNamespace reads a MySQL source URL, mysql://user:pass@host[:3306]/[db]
// (docs/design/mysql-source.md §1): the namespace is "mysql://host:port". The
// database is optional: a MySQL table node already names db.table.
func mysqlNamespace(dsn string) (ns, db string) {
	dsn = strings.TrimSpace(dsn)
	if !strings.HasPrefix(dsn, "mysql://") {
		return "", ""
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" || strings.Contains(u.Host, ",") {
		return "", ""
	}
	if hp := hostPort(u.Host, "3306"); hp != "" {
		return "mysql://" + hp, strings.Trim(u.Path, "/")
	}
	return "", ""
}

// snowflakeNamespace reads the account and database of a gosnowflake DSN,
// user[:password]@<account>/<database>[/<schema>][?...]. The password is
// URL-encoded, so the last '@' ends the credentials.
func snowflakeNamespace(dsn string) (ns, db string) {
	if i := strings.LastIndexByte(dsn, '@'); i >= 0 {
		dsn = dsn[i+1:]
	}
	dsn, _, _ = strings.Cut(dsn, "?")
	parts := strings.Split(dsn, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", ""
	}
	acct := strings.TrimSuffix(strings.ToLower(parts[0]), ".snowflakecomputing.com")
	if strings.ContainsAny(acct, ":/") {
		return "", ""
	}
	return "snowflake://" + acct, parts[1]
}

// hostPort returns "host:port" with def as the port when none is given, or
// "" for an address it cannot read.
func hostPort(addr, def string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host, port = strings.Trim(addr, "[]"), def
	}
	if host == "" || strings.ContainsAny(host, "@/?#") {
		return ""
	}
	return net.JoinHostPort(strings.ToLower(host), port)
}

// dataset returns the OpenLineage namespace and name of a dataset node, or
// ok false for a node this process cannot name: a model (the graph carries
// no relation for it), a node of an unknown or other warehouse, or a kind
// whose namespace is empty.
func (n Namespaces) dataset(node topology.Node) (ns, name string, ok bool) {
	switch node.Kind {
	case topology.KindSourceTable:
		// A MySQL table (node "mysql:<db>.<table>") is named {database}.{table}
		// in a mysql:// namespace; a Postgres one {database}.{schema}.{table}.
		// A node of the other kind than SOURCE_DB_URL is left out.
		if strings.HasPrefix(node.ID, "mysql:") {
			if !strings.HasPrefix(n.Source, "mysql://") || !strings.Contains(node.Name, ".") {
				return "", "", false
			}
			return n.Source, node.Name, true
		}
		if !strings.HasPrefix(node.ID, "pg:") || !strings.HasPrefix(n.Source, "postgres://") {
			return "", "", false
		}
		return n.Source, n.SourceDB + "." + withSchema(node.Name), true
	case topology.KindTopic, topology.KindDLQTopic:
		if n.Kafka == "" || node.Name == "" {
			return "", "", false
		}
		return n.Kafka, node.Name, true
	case topology.KindKafkaEngine, topology.KindMaterializedView:
		if n.WarehouseKind != "clickhouse" {
			return "", "", false
		}
	case topology.KindSinkTable:
		// "wh:<warehouse>:<table>" names a table two warehouses share; only
		// the configured one can be named here.
		if wh, _, dup := strings.Cut(strings.TrimPrefix(node.ID, "wh:"), ":"); dup && wh != "default" && wh != n.WarehouseKind {
			return "", "", false
		}
	default:
		return "", "", false
	}
	if n.Warehouse == "" || node.Name == "" {
		return "", "", false
	}
	if n.WarehouseKind == "clickhouse" {
		switch {
		case strings.Contains(node.Name, "."):
			return n.Warehouse, node.Name, true
		case n.WarehouseDB != "":
			return n.Warehouse, n.WarehouseDB + "." + node.Name, true
		}
		return "", "", false
	}
	table := node.Name
	if n.WarehouseKind == "postgres" {
		table = withSchema(table)
	}
	if n.WarehouseDB == "" || !strings.Contains(table, ".") {
		return "", "", false
	}
	return n.Warehouse, n.WarehouseDB + "." + table, true
}

// withSchema qualifies a bare table name with Postgres's default schema, so
// every name has the {database}.{schema}.{table} shape.
func withSchema(table string) string {
	if strings.Contains(table, ".") {
		return table
	}
	return "public." + table
}
