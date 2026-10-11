-- +goose Up
-- slack_threads links a room to the Slack thread that mirrors it
-- (internal/slackbridge). last_seq is the highest room event the bridge has
-- handled, so a restart resumes the thread instead of posting a new parent.
CREATE TABLE slack_threads (
    room_id    text        PRIMARY KEY REFERENCES rooms (id),
    channel    text        NOT NULL,
    ts         text        NOT NULL,
    last_seq   bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE slack_threads;
