package main

// MongoDB setup breaks (U7c, dev/scenarios.md "Setup breaks: MongoDB"): the
// compose profile "mongo" (a one-member replica set rs0, dev/mongo/README.md)
// made to fail one prerequisite check of internal/clients/mongodb, the client
// the MongoDB setup source reads with. Like the Postgres breaks in
// setupbreaks.go, each scenario is a reversible pair whose verify is the named
// check itself, run through the real client against the profile:
//
//   - standalone-mongod: the server runs without --replSet, so
//     ChangeStreamSupport reports "standalone mongod has no change streams"
//     (check mongo-change-streams);
//   - no-oplog-role: the capture user loses read on local, so RoleInfo
//     reports replication=false: it cannot read local.oplog.rs (check
//     mongo-oplog-read).
//
// The breaks change only the mongo service of the profile, never the Postgres
// pipeline, so they do not need the baseline connector.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"piperoom/internal/clients/mongodb"
)

const (
	mongoContainer = "piperoom-dev-mongo-1"
	mongoService   = "mongo"
	mongoProfile   = "mongo"
	// mongoCaptureRole is the capture user as RoleInfo takes it (authDb.user);
	// dev/mongo/init/01-capture-user.js creates it.
	mongoCaptureRole = "admin.piperoom_cdc"
	// mongoCapturedCollection is the collection the profile seeds, the one a
	// connector on the profile would capture.
	mongoCapturedCollection = "inventory.orders"
	// mongoDevURI is the profile's capture user on the host port. The set's
	// only member is named mongo:27017, which resolves only on the compose
	// network, so a host client connects directly instead of discovering the
	// set. Dev-only credentials (dev/docker-compose.yml), never a secret.
	mongoDevURI = "mongodb://piperoom_cdc:piperoom-dev@127.0.0.1:27017/?authSource=admin&directConnection=true"
	// mongoStartTimeout bounds a mongod restart until the check can read it
	// again. A one-member set elects itself in seconds; the row's bar for the
	// whole profile is two minutes.
	mongoStartTimeout = 2 * time.Minute
	// mongoRecreateHint is how to recreate the profile from nothing. The
	// init script (dev/mongo/init/01-capture-user.js) creates the capture user
	// only on an empty data volume, so a volume from before it lacks the user.
	mongoRecreateHint = "recreate the profile so its init script runs: docker compose -f dev/docker-compose.yml --profile " + mongoProfile + " down -v, then up -d --wait " + mongoService
)

// The named checks of the MongoDB breaks, unique across every setup scenario
// set (a test asserts it).
const (
	checkMongoChangeStreams = "mongo-change-streams"
	checkMongoOplogRead     = "mongo-oplog-read"
)

// mongoSetupScenarios are the two breaks of U7c.
var mongoSetupScenarios = map[string]setupScenario{
	"standalone-mongod": {name: "standalone-mongod", check: checkMongoChangeStreams, breakIt: breakStandaloneMongod, fix: fixStandaloneMongod, verify: verifyMongoChangeStreams},
	"no-oplog-role":     {name: "no-oplog-role", check: checkMongoOplogRead, breakIt: breakNoOplogRole, fix: fixNoOplogRole, verify: verifyMongoOplogRead},
}

// mongoSource is the part of *mongodb.Client the checks read. The unit tests
// fake it, since no mongod runs there.
type mongoSource interface {
	ChangeStreamSupport(ctx context.Context) (mongodb.ChangeStreamSupport, error)
	RoleInfo(ctx context.Context, role string, tables []string) (mongodb.RoleInfo, error)
	Close(ctx context.Context) error
}

// withMongo connects to the profile as the capture user, runs fn and
// disconnects. A fresh connection per check: a role change applies to new
// authentications, and a restarted mongod drops the old ones anyway.
func (s *stack) withMongo(ctx context.Context, fn func(m mongoSource) error) error {
	var (
		m   mongoSource
		err error
	)
	if s.mongoFn != nil {
		m, err = s.mongoFn(ctx)
	} else {
		m, err = mongodb.New(ctx, mongoDevURI)
	}
	if err != nil {
		return fmt.Errorf("devctl: connect to the mongo profile (docker compose --profile %s up -d --wait %s): %w", mongoProfile, mongoService, err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if cerr := m.Close(cctx); cerr != nil {
			s.log.Debug("devctl: closing the mongo client", "err", cerr)
		}
	}()
	return fn(m)
}

// --------------------------------------------------------- standalone mongod

// mongoStandaloneOverride is the Compose override that recreates the mongo
// service without --replSet: the same start script and keyFile (auth stays
// on), the same data volume, only the replica set argument is gone. mongod
// starts on a former member's data with a warning and answers as a
// standalone, which is what a setup run meets on a plain `mongod`.
const mongoStandaloneOverride = `# devctl setup-break standalone-mongod (dev/scenarios.md): the dev mongod
# restarts without --replSet, so it has no oplog-backed change streams.
services:
  mongo:
    command: ["bash", "/piperoom-mongo/start.sh"]
`

// verifyMongoChangeStreams is the mongo-change-streams check: the client's
// ChangeStreamSupport, which a standalone mongod fails.
func verifyMongoChangeStreams(ctx context.Context, s *stack) (bool, string, error) {
	var cs mongodb.ChangeStreamSupport
	if err := s.withMongo(ctx, func(m mongoSource) error {
		var err error
		cs, err = m.ChangeStreamSupport(ctx)
		return err
	}); err != nil {
		return false, "", err
	}
	if !cs.Supported {
		return false, "change streams not supported: " + cs.Reason, nil
	}
	return true, "change streams supported (replica set member)", nil
}

func breakStandaloneMongod(ctx context.Context, s *stack) error {
	dir, err := os.MkdirTemp("", "devctl-mongo-standalone-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	override := filepath.Join(dir, "mongo-standalone.yaml")
	if err := os.WriteFile(override, []byte(mongoStandaloneOverride), 0o600); err != nil {
		return err
	}
	// No --wait: the service's healthcheck wants a primary, which a
	// standalone never is. The check below is the wait.
	if err := s.composeWith(ctx, override, "--profile", mongoProfile, "up", "-d", "--no-deps", mongoService); err != nil {
		return err
	}
	if err := s.waitMongoCheck(ctx, verifyMongoChangeStreams, false); err != nil {
		return fmt.Errorf("mongod did not come back as a standalone: %w", err)
	}
	s.log.Info("broken: mongod runs without --replSet (the setup check " + checkMongoChangeStreams + " fails)")
	return nil
}

// fixStandaloneMongod recreates the service from the base file: --replSet is
// back, the member's replica set config is still on the data volume, so it
// elects itself primary; --wait holds until the healthcheck sees that (and
// initiates rs0 if the config was ever lost).
func fixStandaloneMongod(ctx context.Context, s *stack) error {
	if err := s.composeWith(ctx, "", "--profile", mongoProfile, "up", "-d", "--no-deps", "--wait", mongoService); err != nil {
		return err
	}
	if err := s.waitMongoCheck(ctx, verifyMongoChangeStreams, true); err != nil {
		return fmt.Errorf("mongod did not come back as a replica set member: %w", err)
	}
	s.log.Info("fixed: mongod runs with --replSet rs0 again, change streams supported")
	return nil
}

// waitMongoCheck waits until check reports want. A check that cannot run
// (mongod still starting) is retried until the timeout.
func (s *stack) waitMongoCheck(ctx context.Context, check func(context.Context, *stack) (bool, string, error), want bool) error {
	var last string
	timeout := s.mongoStartTimeout
	if timeout <= 0 {
		timeout = mongoStartTimeout
	}
	err := s.waitFor(ctx, timeout, func() (bool, error) {
		ok, detail, err := check(ctx, s)
		if err != nil {
			last = err.Error()
			return false, nil
		}
		last = detail
		return ok == want, nil
	})
	if err != nil {
		return fmt.Errorf("%w (last: %s)", err, last)
	}
	return nil
}

// ------------------------------------------------------------- no oplog role

// The role edits run inside the container as the profile's root user. They are
// fixed scripts, never text from a request. revokeRolesFromUser and
// grantRolesToUser are idempotent: revoking a role the user lacks, or granting
// one it holds, changes nothing.
const (
	scriptRevokeOplogRead = `db.getSiblingDB('admin').revokeRolesFromUser('piperoom_cdc', [{role: 'read', db: 'local'}])`
	scriptGrantOplogRead  = `db.getSiblingDB('admin').grantRolesToUser('piperoom_cdc', [{role: 'read', db: 'local'}])`
)

// mongoAdminEval is the argv that runs script with mongosh as the profile's
// root user. The credentials never enter the docker exec argv (which shows in
// ps on the host and in devctl's own error text): the shell inside the
// container reads them from the container's environment
// (MONGO_INITDB_ROOT_*, dev/docker-compose.yml). The script is passed as $1,
// never spliced into the shell text.
func mongoAdminEval(script string) []string {
	return []string{"sh", "-c", `exec mongosh --quiet --norc -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --eval "$1"`, "mongosh", script}
}

// verifyMongoOplogRead is the mongo-oplog-read check: the client's RoleInfo
// for the capture user, whose Replication is "can read local.oplog.rs".
func verifyMongoOplogRead(ctx context.Context, s *stack) (bool, string, error) {
	var ri mongodb.RoleInfo
	if err := s.withMongo(ctx, func(m mongoSource) error {
		var err error
		ri, err = m.RoleInfo(ctx, mongoCaptureRole, []string{mongoCapturedCollection})
		return err
	}); err != nil {
		return false, "", err
	}
	if !ri.Exists {
		return false, "capture user " + mongoCaptureRole + " does not exist (" + mongoRecreateHint + ")", nil
	}
	if !ri.Replication {
		return false, "capture user " + mongoCaptureRole + " cannot read local.oplog.rs (no read role on local)", nil
	}
	return true, "capture user " + mongoCaptureRole + " can read local.oplog.rs", nil
}

func breakNoOplogRole(ctx context.Context, s *stack) error {
	if err := s.mongoAdmin(ctx, scriptRevokeOplogRead); err != nil {
		return fmt.Errorf("revoking read on local: %w", err)
	}
	s.log.Info("broken: " + mongoCaptureRole + " lost read on local (the setup check " + checkMongoOplogRead + " fails)")
	return nil
}

func fixNoOplogRole(ctx context.Context, s *stack) error {
	if err := s.mongoAdmin(ctx, scriptGrantOplogRead); err != nil {
		return fmt.Errorf("granting read on local: %w (if the capture user %s is missing, %s)", err, mongoCaptureRole, mongoRecreateHint)
	}
	s.log.Info("fixed: " + mongoCaptureRole + " has read on local again")
	return nil
}

// mongoAdmin runs one fixed admin script in the mongo container.
func (s *stack) mongoAdmin(ctx context.Context, script string) error {
	out, code, err := s.execIn(ctx, mongoContainer, mongoAdminEval(script)...)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("devctl: mongosh in %s: exit %d: %s", mongoContainer, code, strings.TrimSpace(out))
	}
	return nil
}
