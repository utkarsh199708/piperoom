-- The integration and dev reader has only SELECT privileges. readonly=2
-- still permits the client to set its per-query timeout.
CREATE DATABASE IF NOT EXISTS analytics;

CREATE SETTINGS PROFILE IF NOT EXISTS piperoom_reader
    SETTINGS readonly = 2;

CREATE USER IF NOT EXISTS piperoom_reader
    IDENTIFIED WITH sha256_password BY 'piperoom-clickhouse-reader'
    SETTINGS PROFILE piperoom_reader;

GRANT SELECT ON analytics.* TO piperoom_reader;
GRANT SELECT ON system.query_log TO piperoom_reader;
