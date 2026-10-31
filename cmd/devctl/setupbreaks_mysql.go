package main

// MySQL setup breaks (U2b): the `mysql` profile of dev/docker-compose.yml
// (Q8a) made to fail one prerequisite check of the U2a MySQL setup driver
// (internal/setup/sources/mysql), the way setupbreaks.go does it for Postgres.
// Each check name is the driver's own check id, so a scenario label, the
// break's log line and the setup run's failed check name one thing:
//
//   - binlog-statement: binlog_format is STATEMENT, so the driver's
//     source_settings read reports a binary log without row images (check
//     binlog_format). Debezium refuses STATEMENT and MIXED.
//   - no-replication-grant: the connector user 'debezium'@'%' loses
//     REPLICATION CLIENT, so the driver's source_role_info read reports
//     Replication false (check read_role). The incident break
//     missing-replication-grant (Q8a) revokes REPLICATION SLAVE instead; the
//     two touch different privileges, so neither undoes the other.
//   - server-id-zero: server_id is 0, so the server refuses every replication
//     connection (check server_id).
//
// The settings are SET GLOBAL: they hold until the container restarts, which
// starts the profile from its command-line baseline again, so a restart heals
// every break here. Every statement is a fixed constant below; mysql runs in
// the container as root with the container's own MYSQL_ROOT_PASSWORD through
// an option file (scriptMySQLQuery), so devctl never holds or prints it.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// mysqlProfile is the compose profile the MySQL setup breaks run on.
const mysqlProfile = "mysql"

// mysqlBaselineServerID is the profile's --server-id (dev/docker-compose.yml);
// a test asserts the two agree, so the fix restores the value the profile
// starts with.
const mysqlBaselineServerID = 223344

// The MySQL setup check names: the U2a driver's check ids
// (internal/setup/sources/mysql CheckBinlogFormat, CheckReadRole,
// CheckServerID). A test asserts they are the driver's and unique among
// every setup check.
const (
	checkMySQLBinlogFormat = "binlog_format"
	checkMySQLReadRole     = "read_role"
	checkMySQLServerID     = "server_id"
)

// The statements. The reads mirror what the driver's reads report: the global
// binlog_format and server_id (source_settings), and the connector user's
// replication privileges as mysql.user records them (source_role_info).
const (
	sqlMySQLSetupSettings = "SELECT @@GLOBAL.binlog_format, @@GLOBAL.server_id"
	sqlMySQLBinlogRow     = "SET GLOBAL binlog_format = 'ROW'"
	sqlMySQLBinlogStmt    = "SET GLOBAL binlog_format = 'STATEMENT'"
	sqlMySQLServerIDZero  = "SET GLOBAL server_id = 0"
	// sqlMySQLServerIDBase restores the profile's --server-id.
	sqlMySQLServerIDBase = "SET GLOBAL server_id = 223344"

	// sqlMySQLReplPrivs is the connector user's REPLICATION SLAVE and
	// REPLICATION CLIENT, 'Y' or 'N' each; no row when the user is missing.
	sqlMySQLReplPrivs        = "SELECT Repl_slave_priv, Repl_client_priv FROM mysql.user WHERE User = 'debezium' AND Host = '%'"
	sqlMySQLRevokeReplClient = "REVOKE REPLICATION CLIENT ON *.* FROM 'debezium'@'%'"
	sqlMySQLGrantReplClient  = "GRANT REPLICATION CLIENT ON *.* TO 'debezium'@'%'"
)

// mysqlBinlogStatement, mysqlNoReplicationGrant and mysqlServerIDZero are
// registered in setupScenarios (setupbreaks.go).
var (
	mysqlBinlogStatement = setupScenario{
		name: "binlog-statement", check: checkMySQLBinlogFormat, profile: mysqlProfile,
		breakIt: breakMySQLBinlogStatement, fix: fixMySQLBinlogStatement, verify: verifyMySQLBinlogFormat,
	}
	mysqlNoReplicationGrant = setupScenario{
		name: "no-replication-grant", check: checkMySQLReadRole, profile: mysqlProfile,
		breakIt: breakMySQLNoReplicationGrant, fix: fixMySQLNoReplicationGrant, verify: verifyMySQLReadRole,
	}
	mysqlServerIDZero = setupScenario{
		name: "server-id-zero", check: checkMySQLServerID, profile: mysqlProfile,
		breakIt: breakMySQLServerIDZero, fix: fixMySQLServerIDZero, verify: verifyMySQLServerID,
	}
)

// mysqlSetupSettings is what sqlMySQLSetupSettings reads.
type mysqlSetupSettings struct {
	BinlogFormat string
	ServerID     int64
}

// parseMySQLSetupSettings parses the one "<binlog_format>\t<server_id>" row.
func parseMySQLSetupSettings(out string) (mysqlSetupSettings, error) {
	line := lastLine(out)
	parts := strings.Split(line, "\t")
	if len(parts) != 2 {
		return mysqlSetupSettings{}, fmt.Errorf("devctl: unexpected settings row %q", line)
	}
	id, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || id < 0 {
		return mysqlSetupSettings{}, fmt.Errorf("devctl: unexpected server_id %q", parts[1])
	}
	return mysqlSetupSettings{BinlogFormat: strings.ToUpper(strings.TrimSpace(parts[0])), ServerID: id}, nil
}

func (s *stack) mysqlSetupSettings(ctx context.Context) (mysqlSetupSettings, error) {
	out, err := s.mysqlQuery(ctx, sqlMySQLSetupSettings)
	if err != nil {
		return mysqlSetupSettings{}, err
	}
	return parseMySQLSetupSettings(out)
}

// ------------------------------------------------------------ binlog-statement

// verifyMySQLBinlogFormat is the binlog_format check: ROW passes.
func verifyMySQLBinlogFormat(ctx context.Context, s *stack) (bool, string, error) {
	st, err := s.mysqlSetupSettings(ctx)
	if err != nil {
		return false, "", err
	}
	if st.BinlogFormat != "ROW" {
		return false, "binlog_format is " + st.BinlogFormat + "; Debezium needs ROW", nil
	}
	return true, "binlog_format is ROW", nil
}

func breakMySQLBinlogStatement(ctx context.Context, s *stack) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLBinlogStmt); err != nil {
		return fmt.Errorf("setting binlog_format STATEMENT: %w", err)
	}
	s.log.Info("broken: binlog_format is STATEMENT (the setup check " + checkMySQLBinlogFormat + " fails; a container restart restores ROW)")
	return nil
}

func fixMySQLBinlogStatement(ctx context.Context, s *stack) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLBinlogRow); err != nil {
		return fmt.Errorf("setting binlog_format ROW: %w", err)
	}
	s.log.Info("fixed: binlog_format is ROW")
	return nil
}

// -------------------------------------------------------- no-replication-grant

// mysqlReplPrivs is what sqlMySQLReplPrivs reads.
type mysqlReplPrivs struct {
	Exists, Slave, Client bool
}

// parseMySQLReplPrivs parses the "<Y|N>\t<Y|N>" row; no row means the user
// does not exist.
func parseMySQLReplPrivs(out string) (mysqlReplPrivs, error) {
	line := lastLine(out)
	if line == "" {
		return mysqlReplPrivs{}, nil
	}
	parts := strings.Split(line, "\t")
	if len(parts) != 2 {
		return mysqlReplPrivs{}, fmt.Errorf("devctl: unexpected privilege row %q", line)
	}
	yn := func(v string) (bool, error) {
		switch strings.TrimSpace(v) {
		case "Y":
			return true, nil
		case "N":
			return false, nil
		}
		return false, fmt.Errorf("devctl: unexpected privilege value %q", v)
	}
	slave, err1 := yn(parts[0])
	client, err2 := yn(parts[1])
	if err1 != nil || err2 != nil {
		return mysqlReplPrivs{}, fmt.Errorf("devctl: unexpected privilege row %q", line)
	}
	return mysqlReplPrivs{Exists: true, Slave: slave, Client: client}, nil
}

// verifyMySQLReadRole is the read_role check's replication half: the
// connector user exists and holds REPLICATION SLAVE and REPLICATION CLIENT.
func verifyMySQLReadRole(ctx context.Context, s *stack) (bool, string, error) {
	out, err := s.mysqlQuery(ctx, sqlMySQLReplPrivs)
	if err != nil {
		return false, "", err
	}
	p, err := parseMySQLReplPrivs(out)
	if err != nil {
		return false, "", err
	}
	if !p.Exists {
		return false, "the connector user 'debezium'@'%' does not exist (dev/mysql/init did not run; recreate the container)", nil
	}
	var missing []string
	if !p.Slave {
		missing = append(missing, "REPLICATION SLAVE")
	}
	if !p.Client {
		missing = append(missing, "REPLICATION CLIENT")
	}
	if len(missing) > 0 {
		return false, "'debezium'@'%' is missing " + strings.Join(missing, " and "), nil
	}
	return true, "'debezium'@'%' holds REPLICATION SLAVE and REPLICATION CLIENT", nil
}

func breakMySQLNoReplicationGrant(ctx context.Context, s *stack) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLRevokeReplClient); err != nil {
		return fmt.Errorf("revoking REPLICATION CLIENT: %w", err)
	}
	s.log.Info("broken: 'debezium'@'%' has no REPLICATION CLIENT (the setup check " + checkMySQLReadRole + " fails)")
	return nil
}

func fixMySQLNoReplicationGrant(ctx context.Context, s *stack) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLGrantReplClient); err != nil {
		return fmt.Errorf("granting REPLICATION CLIENT: %w", err)
	}
	s.log.Info("fixed: 'debezium'@'%' holds REPLICATION CLIENT again")
	return nil
}

// -------------------------------------------------------------- server-id-zero

// verifyMySQLServerID is the server_id check's server half: non-zero passes.
// (The driver also refuses a server_id equal to the connector's
// database.server.id; the dev connector's id is not this server's.)
func verifyMySQLServerID(ctx context.Context, s *stack) (bool, string, error) {
	st, err := s.mysqlSetupSettings(ctx)
	if err != nil {
		return false, "", err
	}
	if st.ServerID == 0 {
		return false, "server_id is 0; the server refuses every replication connection", nil
	}
	return true, "server_id is " + strconv.FormatInt(st.ServerID, 10), nil
}

func breakMySQLServerIDZero(ctx context.Context, s *stack) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLServerIDZero); err != nil {
		return fmt.Errorf("setting server_id 0: %w", err)
	}
	s.log.Info("broken: server_id is 0 (the setup check " + checkMySQLServerID + " fails; a container restart restores the profile's id)")
	return nil
}

func fixMySQLServerIDZero(ctx context.Context, s *stack) error {
	if _, err := s.mysqlQuery(ctx, sqlMySQLServerIDBase); err != nil {
		return fmt.Errorf("restoring server_id: %w", err)
	}
	s.log.Info("fixed: server_id is " + strconv.Itoa(mysqlBaselineServerID))
	return nil
}
