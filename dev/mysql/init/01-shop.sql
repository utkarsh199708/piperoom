-- MySQL source of the dev stack (Q8a, docs/design/mysql-source.md §10.1).
-- Runs once, as root, on a fresh data directory (the image's entrypoint).
-- Passwords are dev-only (AGENTS.md §3; dev/mysql/README.md lists them).

CREATE DATABASE shop;

CREATE TABLE shop.orders (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  amount      INT NOT NULL,
  status      VARCHAR(32) NOT NULL DEFAULT 'placed',
  updated_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
);

CREATE TABLE shop.order_items (
  id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  sku      VARCHAR(64) NOT NULL,
  qty      INT NOT NULL,
  KEY order_items_order (order_id)
);

-- The connector user, with the grants of Debezium's MySQL connector docs
-- (design §10.2). `devctl break missing-replication-grant` revokes
-- REPLICATION SLAVE and its fix grants it back.
CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz-dev';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';

-- Piperoom's read user (SOURCE_DB_URL on a MySQL pipeline, Q8b): no
-- REPLICATION SLAVE (Piperoom reads no binlog stream) and only the listed
-- mysql.user columns, never authentication_string (design §10.2).
CREATE USER 'piperoom'@'%' IDENTIFIED BY 'piperoom-dev';
GRANT SELECT ON shop.orders TO 'piperoom'@'%';
GRANT SELECT ON shop.order_items TO 'piperoom'@'%';
GRANT REPLICATION CLIENT, PROCESS ON *.* TO 'piperoom'@'%';
GRANT SELECT ON performance_schema.* TO 'piperoom'@'%';
GRANT SELECT (User, Host, account_locked, password_expired, Repl_slave_priv, Repl_client_priv,
              Select_priv, Reload_priv, Lock_tables_priv, Show_db_priv) ON mysql.user TO 'piperoom'@'%';
GRANT SELECT ON mysql.db TO 'piperoom'@'%';
GRANT SELECT ON mysql.tables_priv TO 'piperoom'@'%';
GRANT SELECT ON mysql.role_edges TO 'piperoom'@'%';
GRANT SELECT ON mysql.default_roles TO 'piperoom'@'%';

-- Piperoom's write user (SOURCE_DB_WRITE_URL, Q8f): grant SELECT on the
-- captured tables, end sessions, create the heartbeat table (design §6.8).
CREATE DATABASE piperoom_heartbeat;
CREATE USER 'piperoom_write'@'%' IDENTIFIED BY 'piperoom-dev-write';
GRANT SELECT ON shop.orders TO 'piperoom_write'@'%' WITH GRANT OPTION;
GRANT SELECT ON shop.order_items TO 'piperoom_write'@'%' WITH GRANT OPTION;
GRANT CONNECTION_ADMIN, PROCESS, REPLICATION CLIENT ON *.* TO 'piperoom_write'@'%';
GRANT CREATE, INSERT, UPDATE, SELECT ON piperoom_heartbeat.* TO 'piperoom_write'@'%';
GRANT SELECT ON performance_schema.* TO 'piperoom_write'@'%';
GRANT SELECT (User, Host, account_locked, password_expired, Repl_slave_priv, Repl_client_priv,
              Select_priv, Reload_priv, Lock_tables_priv, Show_db_priv) ON mysql.user TO 'piperoom_write'@'%';

-- Seed rows last: the compose healthcheck waits for them, so "healthy" means
-- this whole script ran.
INSERT INTO shop.order_items (order_id, sku, qty)
  SELECT n, CONCAT('sku-', n % 7), 1 + n % 3 FROM (
    SELECT a.d * 10 + b.d + 1 AS n
    FROM (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
         (SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
  ) seq;
INSERT INTO shop.orders (customer_id, amount)
  SELECT order_id, 100 + order_id FROM shop.order_items ORDER BY order_id;
