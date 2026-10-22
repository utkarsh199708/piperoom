-- +goose Up
-- Watch sample history (X3a; clients.SampleWriter / SampleReader): one number
-- per pipeline, metric and time, written by the watcher at most every 15
-- minutes and kept 7 days (config.WatchSampleRetention; the samples retention
-- loop in cmd/server deletes older rows). Never a row value, a record, a
-- config value or free text. The primary key makes a second sample with the
-- same (pipeline, metric, at) a no-op and serves the trends read
-- (WHERE pipeline_id, metric, at >= since ORDER BY at).
CREATE TABLE watch_samples (
    pipeline_id text             NOT NULL,
    metric      text             NOT NULL,
    at          timestamptz      NOT NULL,
    value       double precision NOT NULL,
    PRIMARY KEY (pipeline_id, metric, at)
);
-- Retention deletes by age across pipelines.
CREATE INDEX watch_samples_at ON watch_samples (at);

-- +goose Down
DROP TABLE IF EXISTS watch_samples;
