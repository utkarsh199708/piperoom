-- +goose Up
-- Durable model spend (docs/design/cost.md section 3.2). room_spend and
-- pipeline_spend_daily hold what the caps check: tokens and cost charged so far,
-- including the estimate of a request still in flight (charged at admit,
-- settled after it). The *_models tables hold finished requests by model for
-- the usage routes. Cost is in millionths of a USD, estimated from
-- PIPEROOM_MODEL_PRICES (0 for a request whose model has no price; such
-- requests are counted in unpriced_calls). Every writer locks room_spend before
-- pipeline_spend_daily, so concurrent charges cannot deadlock. A purged room
-- takes its two room tables with it; the pipeline tables are kept.
CREATE TABLE room_spend (
    room_id     text        PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
    tokens      bigint      NOT NULL DEFAULT 0,
    cost_micros bigint      NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE room_spend_models (
    room_id                     text   NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    model                       text   NOT NULL,
    calls                       bigint NOT NULL DEFAULT 0,
    unpriced_calls              bigint NOT NULL DEFAULT 0,
    input_tokens                bigint NOT NULL DEFAULT 0,
    cache_creation_input_tokens bigint NOT NULL DEFAULT 0,
    cache_read_input_tokens     bigint NOT NULL DEFAULT 0,
    output_tokens               bigint NOT NULL DEFAULT 0,
    cost_micros                 bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (room_id, model)
);

CREATE TABLE pipeline_spend_daily (
    pipeline_id text        NOT NULL,
    day         date        NOT NULL,   -- UTC
    tokens      bigint      NOT NULL DEFAULT 0,
    cost_micros bigint      NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, day)
);

CREATE TABLE pipeline_spend_daily_models (
    pipeline_id                 text   NOT NULL,
    day                         date   NOT NULL,
    model                       text   NOT NULL,
    calls                       bigint NOT NULL DEFAULT 0,
    unpriced_calls              bigint NOT NULL DEFAULT 0,
    input_tokens                bigint NOT NULL DEFAULT 0,
    cache_creation_input_tokens bigint NOT NULL DEFAULT 0,
    cache_read_input_tokens     bigint NOT NULL DEFAULT 0,
    output_tokens               bigint NOT NULL DEFAULT 0,
    cost_micros                 bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline_id, day, model)
);

-- +goose Down
DROP TABLE IF EXISTS pipeline_spend_daily_models;
DROP TABLE IF EXISTS pipeline_spend_daily;
DROP TABLE IF EXISTS room_spend_models;
DROP TABLE IF EXISTS room_spend;
