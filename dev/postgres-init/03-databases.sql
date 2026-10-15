-- The dev warehouse (sink of the pipeline). The room service's database is not
-- here: it is the separate `roomdb` service (localhost:5433, db piperoom), so a
-- pipeline reset never deletes rooms (QA G19, dev/scenarios.md "Room database").
CREATE DATABASE warehouse;

\connect warehouse

CREATE SCHEMA analytics;

CREATE TABLE analytics.orders (
  id          BIGINT PRIMARY KEY,
  customer_id BIGINT,
  amount      NUMERIC(12,2),
  status      TEXT,
  updated_at  TIMESTAMPTZ,
  loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
