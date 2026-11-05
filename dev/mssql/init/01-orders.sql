-- SQL Server source of the dev stack (U4c, dev/mssql/README.md). The image's
-- setup hook runs this once, on a fresh data directory, as sa. CDC stays off on
-- purpose: enabling it (sp_cdc_enable_db, sp_cdc_enable_table) is the setup
-- run's job, and `devctl setup-break cdc-not-enabled-table` sets up the
-- half-enabled state itself.
IF DB_ID(N'orders') IS NULL
    CREATE DATABASE orders;
GO

USE orders;
GO

-- The same shape as the Postgres orders table (dev/postgres-init/01-orders.sql).
-- A primary key, because Debezium's SQL Server connector keys change events on it.
IF OBJECT_ID(N'dbo.orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.orders (
        id          bigint IDENTITY(1, 1) NOT NULL CONSTRAINT pk_orders PRIMARY KEY,
        customer_id bigint         NOT NULL,
        amount      int            NOT NULL,
        status      nvarchar(32)   NOT NULL CONSTRAINT df_orders_status DEFAULT N'placed',
        updated_at  datetime2(6)   NOT NULL CONSTRAINT df_orders_updated_at DEFAULT SYSUTCDATETIME()
    );

    INSERT INTO dbo.orders (customer_id, amount)
    SELECT ABS(CHECKSUM(NEWID())) % 1000, ABS(CHECKSUM(NEWID())) % 5000
    FROM (SELECT TOP (100) 1 AS n FROM sys.all_objects) AS rows100;
END
GO
