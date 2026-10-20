// Package mongodb implements a read-only client for a MongoDB CDC source (the
// Debezium MongoDB connector reads the oplog / change streams). It is
// read-only by construction: every exported method is a read command
// (isMaster, buildInfo, replSetGetStatus, connectionStatus); there is no write
// method. The connecting role must therefore be granted only the read roles it
// needs (read on the captured databases, and find/changeStream on
// local.oplog.rs for the cursor mode).
//
// Driver: go.mongodb.org/mongo-driver/v2 v2.9.2, the official MongoDB Go
// driver (see the PR's "New dependencies"). v2's mongo.Connect takes no ctx
// and v2 decodes nested documents in a bson.M as bson.D by default, so the
// client opts in to options.BSONOptions{DefaultDocumentM: true} to keep the
// asMap/asSlice helpers decoding documents into bson.M. Each call runs under
// its own 10 s deadline (defaultTimeout). The connection string may carry
// credentials, so it is never put into an error message or log line: errors
// are wrapped through redactErr and any logging uses redactURI.
//
// The client implements SourceSettingsReader, a local mirror of the interface
// proposed for clients.SourceSettingsReader in tasks/Q-board.md Q2a (it is not
// yet in internal/clients/clients.go — see the PR's "Contract change request").
// Settings reports the version, server type, replica set state and
// change-stream support; RoleInfo reports the authenticated user's roles,
// whether they can read the oplog, and per-collection read ability.
// TableReplication and Blockers have no MongoDB equivalent and answer
// clients.ErrNotSupported.
package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// defaultTimeout is the per-call deadline (10 s per call, AGENTS.md rule 4).
const defaultTimeout = 10 * time.Second

// runner is the subset of the MongoDB driver the client needs. It is the seam
// the unit tests fake: every read goes through it, so no test needs a real
// mongod. Implemented by driverClient and by fakes in _test.go.
type runner interface {
	runCommand(ctx context.Context, db string, cmd bson.D) (bson.M, error)
	disconnect(ctx context.Context) error
}

// Client is a read-only MongoDB client for the CDC source.
type Client struct {
	run runner
}

// driverClient adapts *mongo.Client to runner.
type driverClient struct {
	cli *mongo.Client
}

func (d *driverClient) runCommand(ctx context.Context, db string, cmd bson.D) (bson.M, error) {
	var out bson.M
	if err := d.cli.Database(db).RunCommand(ctx, cmd).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (d *driverClient) disconnect(ctx context.Context) error {
	return d.cli.Disconnect(ctx)
}

// New connects to uri and verifies it with a ping under a 10 s deadline. uri
// is a MongoDB connection string (mongodb:// or mongodb+srv://); it may carry
// credentials, so any error leaving New is redacted.
func New(ctx context.Context, uri string) (*Client, error) {
	opts := options.Client().ApplyURI(uri).
		SetConnectTimeout(defaultTimeout).
		SetServerSelectionTimeout(defaultTimeout).
		SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true})
	cli, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: connect: %w", redactErr(err, uri))
	}
	pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if err := cli.Ping(pctx, readpref.Primary()); err != nil {
		dctx, dcancel := context.WithTimeout(ctx, defaultTimeout)
		_ = cli.Disconnect(dctx)
		dcancel()
		return nil, fmt.Errorf("mongodb: ping: %w", redactErr(err, uri))
	}
	return &Client{run: &driverClient{cli: cli}}, nil
}

// Close disconnects the client. It is safe to call more than once.
func (c *Client) Close(ctx context.Context) error {
	return c.run.disconnect(ctx)
}

// cmd runs one read command against db under a fresh 10 s deadline.
func (c *Client) cmd(ctx context.Context, db string, d bson.D) (bson.M, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	return c.run.runCommand(ctx, db, d)
}
