-- +goose Up
-- One Slack thread per room (internal/slackbridge). last_seq is the highest
-- room event already mirrored, so a restart resumes instead of re-posting.
CREATE TABLE slack_threads (
    room_id     text        PRIMARY KEY REFERENCES rooms(id) ON DELETE CASCADE,
    channel     text        NOT NULL,
    ts          text        NOT NULL,
    last_seq    bigint      NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS slack_threads;
