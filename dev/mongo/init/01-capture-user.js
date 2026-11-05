// Runs once, on an empty data directory (docker-entrypoint.sh, before the
// replica set exists). Dev-only credentials, never real secrets.
//
// piperoom_cdc is the capture user a Debezium MongoDB connector would use:
// readAnyDatabase (listDatabases, find and changeStream on every user
// database) plus read on local, which is what makes local.oplog.rs readable:
// the any-database roles never cover local, config or admin. read on config
// is in the Debezium connector's documented privileges too.
// `devctl setup-break no-oplog-role` revokes read on local, and the mongodb
// client's RoleInfo then reports replication=false.
const admin = db.getSiblingDB("admin");
if (admin.getUser("piperoom_cdc") === null) {
  admin.createUser({
    user: "piperoom_cdc",
    pwd: "piperoom-dev",
    roles: [
      { role: "readAnyDatabase", db: "admin" },
      { role: "read", db: "local" },
      { role: "read", db: "config" },
    ],
  });
}

// A small captured collection, so a connector or a canary has something to
// read on a fresh profile.
const inventory = db.getSiblingDB("inventory");
if (inventory.orders.countDocuments({}) === 0) {
  inventory.orders.insertMany([
    { _id: 1, customer: "alice", amount_cents: 1250, status: "new" },
    { _id: 2, customer: "bob", amount_cents: 980, status: "paid" },
    { _id: 3, customer: "carol", amount_cents: 4400, status: "shipped" },
  ]);
}
