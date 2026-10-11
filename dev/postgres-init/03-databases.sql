-- The dev warehouse (sink of the pipeline) and the room service database.
CREATE DATABASE warehouse;
CREATE DATABASE piperoom;

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
