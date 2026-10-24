package wiring

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/events"
)

// Write mode (PIPEROOM_WRITE_MODE, P0 item 1; P1a-3).
//
// Off, the default: no write client is constructed at all, and the actions are
// registered over read-only Connect and Schema Registry clients
// (readOnlyActionConnect, readOnlyActionRegistry), so even an action that the
// executor somehow ran could not write. The agent still proposes from that
// registry: Validate and Preview only read. On: the actions get the whole
// clients, and each client set also gets the P3 writers below.

// closingSourceWriter and closingKafkaWriter are the write clients as wiring
// holds them: the contract interface plus the Close the set needs.
type (
	closingSourceWriter interface {
		clients.SourceWriter
		Close()
	}
	closingKafkaWriter interface {
		clients.KafkaWriter
		Close()
	}
	// closingAdminWriter is a MySQL source's writer (Q8f, Q8g): the source
	// admin methods only, since MySQL has no slot, publication or signal
	// table for clients.SourceWriter.
	closingAdminWriter interface {
		clients.SourceAdminWriter
		Close()
	}
)

// writeClients are one set's P3 write clients, built only with write mode on.
// Only package actions may be handed them (clients.go, Reader/Writer split).
type writeClients struct {
	// source is nil without SOURCE_DB_WRITE_URL: the pg actions then refuse
	// with "no write credentials". It is the Postgres writer, so it is nil on
	// a MySQL set too.
	source closingSourceWriter
	// admin is the MySQL source writer (mysql.NewWriter) of a MySQL set with
	// SOURCE_DB_WRITE_URL; nil otherwise. On a Postgres set the admin writer
	// is source itself (adminWriter).
	admin closingAdminWriter
	kafka closingKafkaWriter
}

// writerOpeners build the write clients; tests replace them so no stack is
// needed.
type writerOpeners struct {
	source func(ctx context.Context, dsn string) (closingSourceWriter, error)
	// mysql builds a MySQL set's source writer; readURL is the set's read
	// URL, whose user the writer protects (mysql.WithReaderUser).
	mysql func(ctx context.Context, dsn, readURL string) (closingAdminWriter, error)
	kafka func(brokers []string, sec netsec.Kafka) (closingKafkaWriter, error)
}

// defaultWriterOpeners are the real constructors. A nil concrete writer is
// never returned inside the interface: a caller checking "!= nil" would think
// it holds one.
func defaultWriterOpeners() writerOpeners {
	return writerOpeners{
		source: func(ctx context.Context, dsn string) (closingSourceWriter, error) {
			w, err := postgres.NewWriter(ctx, dsn)
			if err != nil {
				return nil, err
			}
			return w, nil
		},
		mysql: func(ctx context.Context, dsn, readURL string) (closingAdminWriter, error) {
			w, err := mysql.NewWriter(ctx, dsn, mysqlWriterOptions(readURL)...)
			if err != nil {
				return nil, err
			}
			return w, nil
		},
		kafka: func(brokers []string, sec netsec.Kafka) (closingKafkaWriter, error) {
			w, err := kafka.NewSecureWriter(brokers, sec)
			if err != nil {
				return nil, err
			}
			return w, nil
		},
	}
}

// openWriters builds the write clients of one client set from cfg, for write
// mode on only: the Kafka writer over the set's own brokers and security
// (connections of its own, not shared with the reader the tools hold; no I/O
// until first use), and the source Postgres writer on cfg.SourceDBWriteURL
// when that is set (it connects and refuses a read-only role or a standby):
// SOURCE_DB_WRITE_URL for the process-wide set, and for a pipeline with its
// own connections: block that pipeline's connections.source_db_write_url
// (pipelineConfig), never the process-wide one. It returns a closer for what
// it opened; on error nothing stays open and the error never carries a DSN.
// It is openWritersFor a Postgres set.
func openWriters(ctx context.Context, cfg config.Config, awsCfg *aws.Config, op writerOpeners) (writeClients, func(), error) {
	return openWritersFor(ctx, cfg, awsCfg, op, events.SourceKindPostgres)
}

// openWritersFor is openWriters for a set of the given source kind
// (checkSourceURLs): on mysql the source writer is mysql.NewWriter's,
// protecting the read URL's user, and is held as the admin writer only.
func openWritersFor(ctx context.Context, cfg config.Config, awsCfg *aws.Config, op writerOpeners, sourceKind string) (writeClients, func(), error) {
	if err := checkSourceWriteURL(cfg); err != nil {
		return writeClients{}, nil, err
	}
	sec, err := kafkaSecurity(cfg, awsCfg)
	if err != nil {
		return writeClients{}, nil, err
	}
	kw, err := op.kafka(splitBrokers(cfg.KafkaBrokers), sec)
	if err != nil {
		return writeClients{}, nil, fmt.Errorf("wiring: kafka writer: %w", err)
	}
	wc := writeClients{kafka: kw}
	if strings.TrimSpace(cfg.SourceDBWriteURL) == "" {
		return wc, kw.Close, nil
	}
	if sourceKind == events.SourceKindMySQL {
		if op.mysql == nil {
			kw.Close()
			return writeClients{}, nil, errors.New("wiring: source mysql writer: no MySQL writer constructor")
		}
		aw, err := op.mysql(ctx, cfg.SourceDBWriteURL, cfg.SourceDBURL)
		if err != nil {
			kw.Close()
			// mysql.NewWriter never puts the URL in its errors.
			return writeClients{}, nil, fmt.Errorf("wiring: source mysql writer: %w", err)
		}
		wc.admin = aw
		return wc, func() { aw.Close(); kw.Close() }, nil
	}
	sw, err := op.source(ctx, cfg.SourceDBWriteURL)
	if err != nil {
		kw.Close()
		// postgres.NewWriter already strips the DSN from its errors.
		return writeClients{}, nil, fmt.Errorf("wiring: source postgres writer: %w", err)
	}
	wc.source = sw
	return wc, func() { sw.Close(); kw.Close() }, nil
}

// errSameSourceRole wraps the checkSourceWriteURL refusal.
var errSameSourceRole = errors.New("SOURCE_DB_WRITE_URL is the SOURCE_DB_URL role")

// errSamePipelineSourceRole wraps the checkPipelineWriteURL refusal.
var errSamePipelineSourceRole = errors.New("connections.source_db_write_url is the source_db_url role")

// checkSourceWriteURL refuses a SOURCE_DB_WRITE_URL that logs in as the
// read-only SOURCE_DB_URL role: the same text, or the same user on the same
// host and port as pgx parses them (a role belongs to the server, so another
// database, password or option does not make it another role). The source
// writer must have a write role of its own (clients.SourceWriter). It runs in
// both write modes, so the mistake stops the start before anyone turns write
// mode on during an incident. The error names the variables, never a value.
func checkSourceWriteURL(cfg config.Config) error {
	if writeIsReadRole(cfg) {
		return fmt.Errorf("wiring: SOURCE_DB_WRITE_URL logs in as the SOURCE_DB_URL role (same user, host and port; values not shown): "+
			"the source writer needs a write role of its own, never the read-only one: %w", errSameSourceRole)
	}
	return nil
}

// checkPipelineWriteURL is checkSourceWriteURL for the resolved settings of a
// pipeline with its own connections: block (P1a-3c). owners.Load can only
// refuse one env: or file: reference named twice; two references that
// resolve to the same DSN, or to the same role, are caught here, once both
// are resolved. Like the process-wide check it runs in both write modes. The
// error names the pipeline and the keys, never a value.
func checkPipelineWriteURL(id string, pc config.Config) error {
	if writeIsReadRole(pc) {
		return fmt.Errorf("wiring: pipeline %q: connections.source_db_write_url logs in as the connections.source_db_url role "+
			"(same user, host and port; values not shown): the source writer needs a write role of its own, never the read-only one: %w",
			id, errSamePipelineSourceRole)
	}
	return nil
}

// writeIsReadRole reports whether cfg's source write URL is set and logs in
// as its read URL: the same text, or the same role (sameSourceRole).
func writeIsReadRole(cfg config.Config) bool {
	wr := strings.TrimSpace(cfg.SourceDBWriteURL)
	if wr == "" {
		return false
	}
	rd := strings.TrimSpace(cfg.SourceDBURL)
	return wr == rd || sameSourceRole(rd, wr) || sameMySQLRole(rd, wr)
}

// sameSourceRole reports whether two DSNs name the same user on the same
// first host and port. A DSN that does not parse is not judged here (false):
// the writer reports it when it connects, with the DSN removed.
func sameSourceRole(read, write string) bool {
	r, err := pgconn.ParseConfig(read)
	if err != nil {
		return false
	}
	w, err := pgconn.ParseConfig(write)
	if err != nil {
		return false
	}
	return r.User == w.User && r.Host == w.Host && r.Port == w.Port
}

// openWriteMode reads PIPEROOM_WRITE_MODE for OpenStacks and Open: a bad value
// stops the start, readOnly (StackOptions.ReadOnly) can only turn writes off,
// and a SOURCE_DB_WRITE_URL equal to SOURCE_DB_URL is refused in either mode.
func openWriteMode(cfg config.Config, readOnly bool) (bool, error) {
	on, err := config.ParseWriteMode(cfg)
	if err != nil {
		return false, fmt.Errorf("wiring: %w", err)
	}
	if err := checkSourceWriteURL(cfg); err != nil {
		return false, err
	}
	return on && !readOnly, nil
}
