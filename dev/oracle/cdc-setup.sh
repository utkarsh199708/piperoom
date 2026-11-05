#!/bin/bash
# Oracle CDC prerequisites for the dev `oracle` profile (U9c, dev/oracle/README.md).
#
# Runs once, on the container's first start, from
# /container-entrypoint-initdb.d/01-cdc-setup.sh (the gvenzl image sources that
# file as the `oracle` OS user after the database is open; a non-zero exit here
# aborts the container start, so a half-configured database never turns
# healthy). Every step checks before it changes anything, so running it again
# (`docker exec piperoom-dev-oracle-1 bash /opt/piperoom/oracle/cdc-setup.sh`)
# is safe and restores whatever a `devctl setup-break` took away.
#
# The healthy state is the one the Debezium Oracle connector documents and the
# U9a client (internal/clients/oracle) checks:
#   - ARCHIVELOG mode, archives in a recovery area under oradata;
#   - minimal database-level supplemental logging, and an ALL COLUMNS log group
#     on PIPEROOM.ORDERS (the captured table, in FREEPDB1);
#   - the common LogMiner user C##DBZUSER with the grants of the Debezium docs
#     (a superset of what U9a checks), plus SELECT on PIPEROOM.ORDERS.
# Passwords are the dev stack's (AGENTS.md section 3), never real secrets.
set -euo pipefail

PDB="${PIPEROOM_ORACLE_PDB:-FREEPDB1}"
FRA="${ORACLE_BASE:-/opt/oracle}/oradata/FRA"

# sysdba runs the SQL*Plus script on stdin as SYS (OS authentication: the
# `oracle` user is in the dba group). Any SQL error exits non-zero.
sysdba() {
  sqlplus -S -L / as sysdba
}

# value prints one value of a single-row query, blanks trimmed.
value() {
  printf 'SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF ECHO OFF\nWHENEVER SQLERROR EXIT FAILURE\n%s\nEXIT\n' "$1" |
    sysdba | tr -d '[:space:]'
}

echo "piperoom: oracle CDC setup (pdb ${PDB})"

# 1. Archive log mode. ALTER DATABASE ARCHIVELOG runs only on a MOUNTed
# instance, so the database restarts once; on a database already in
# ARCHIVELOG nothing restarts. The recovery area is set first so the archives
# land under oradata (the default, ?/dbs/arch, is inside ORACLE_HOME).
mkdir -p "${FRA}"
if [ "$(value "SELECT log_mode FROM v\$database;")" != "ARCHIVELOG" ]; then
  echo "piperoom: enabling ARCHIVELOG (one restart)"
  sysdba <<EOF
WHENEVER SQLERROR EXIT FAILURE
ALTER SYSTEM SET db_recovery_file_dest_size = 10G SCOPE=BOTH;
ALTER SYSTEM SET db_recovery_file_dest = '${FRA}' SCOPE=BOTH;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
EXIT
EOF
fi
if [ "$(value "SELECT log_mode FROM v\$database;")" != "ARCHIVELOG" ]; then
  echo "piperoom: the database is not in ARCHIVELOG mode after the restart" >&2
  exit 1
fi

# 2. Minimal database-level supplemental logging (CDB-wide in a CDB).
sysdba <<'EOF'
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
DECLARE
  v_min VARCHAR2(8);
BEGIN
  SELECT supplemental_log_data_min INTO v_min FROM v$database;
  IF v_min = 'NO' THEN
    EXECUTE IMMEDIATE 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA';
  END IF;
END;
/
EXIT
EOF

# 3. The common LogMiner user and its grants, in every container. Grants are
# idempotent; the user is created only when missing (its password is never
# reset by a rerun).
sysdba <<'EOF'
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_users WHERE username = 'C##DBZUSER';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER c##dbzuser IDENTIFIED BY "piperoom-dev" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users CONTAINER=ALL';
  END IF;
END;
/
GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$THREAD TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$MYSTAT TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$STATNAME TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_TABLES TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_TAB_COLUMNS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_INDEXES TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_TRIGGERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_CONSTRAINTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_CONS_COLUMNS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_LOG_GROUPS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON DBA_REGISTRY TO c##dbzuser CONTAINER=ALL;
EXIT
EOF

# 4. The captured table in the PDB: PIPEROOM.ORDERS (the Postgres orders
# table's shape), its ALL COLUMNS supplemental log group, SELECT for the
# LogMiner user, and a few rows so a snapshot has something to read.
sysdba <<EOF
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
ALTER SESSION SET CONTAINER = ${PDB};
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_users WHERE username = 'PIPEROOM';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER piperoom IDENTIFIED BY "piperoom-dev" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users';
  END IF;
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE TO piperoom';
  SELECT COUNT(*) INTO n FROM dba_tables WHERE owner = 'PIPEROOM' AND table_name = 'ORDERS';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE piperoom.orders ('
      || 'id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, '
      || 'customer_id NUMBER(19) NOT NULL, '
      || 'amount NUMBER(10) NOT NULL, '
      || 'status VARCHAR2(32) DEFAULT ''placed'' NOT NULL, '
      || 'updated_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL)';
    EXECUTE IMMEDIATE 'INSERT INTO piperoom.orders (customer_id, amount) '
      || 'SELECT MOD(LEVEL * 37, 1000), MOD(LEVEL * 131, 5000) FROM dual CONNECT BY LEVEL <= 100';
    COMMIT;
  END IF;
  SELECT COUNT(*) INTO n FROM dba_log_groups
   WHERE owner = 'PIPEROOM' AND table_name = 'ORDERS'
     AND log_group_type = 'ALL COLUMN LOGGING' AND always = 'ALWAYS';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE piperoom.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
  END IF;
END;
/
GRANT SELECT ON piperoom.orders TO c##dbzuser;
EXIT
EOF

echo "piperoom: oracle CDC setup done (ARCHIVELOG, supplemental logging, C##DBZUSER)"
