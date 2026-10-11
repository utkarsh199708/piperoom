-- Sample source table for the CDC pipeline.
CREATE TABLE orders (
  id          BIGSERIAL PRIMARY KEY,
  customer_id BIGINT      NOT NULL,
  amount      INTEGER     NOT NULL,
  status      TEXT        NOT NULL DEFAULT 'placed',
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders (customer_id, amount, status)
SELECT (random() * 1000)::bigint, (random() * 5000)::int, 'placed'
FROM generate_series(1, 100);
