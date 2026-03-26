/*
sp_IndexCleanup Adversarial Test Suite — Setup & Execute
========================================================
This script:
  1. Creates test tables with data and edge-case index configurations
  2. Generates usage stats
  3. Runs sp_IndexCleanup @dedupe_only = 1

Output is captured by the test runner (run_tests.py) for validation.
Run with: python run_tests.py

Direct execution (visual inspection only):
  sqlcmd -S SQL2022 -U sa -P "password" -d StackOverflow2013 -i adversarial_test.sql
*/
SET NOCOUNT ON;

USE StackOverflow2013;
GO

/* ============================================= */
/* Cleanup previous test artifacts               */
/* ============================================= */
IF OBJECT_ID('dbo.test_ic_view') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1/0 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.test_ic_view') AND name = N'cx_test_ic_view')
        DROP INDEX cx_test_ic_view ON dbo.test_ic_view;
END;
GO
IF OBJECT_ID('dbo.test_ic_view') IS NOT NULL DROP VIEW dbo.test_ic_view;
GO
DROP TABLE IF EXISTS dbo.test_ic_basic;
DROP TABLE IF EXISTS dbo.test_ic_uc;
DROP TABLE IF EXISTS dbo.test_ic_filtered;
DROP TABLE IF EXISTS dbo.test_ic_heap;
DROP TABLE IF EXISTS dbo.test_ic_multi;
DROP TABLE IF EXISTS dbo.test_ic_view_base;
GO

/* ============================================= */
/* Create test tables with data                  */
/* ============================================= */

CREATE TABLE dbo.test_ic_basic
(
    id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
    col_a integer NOT NULL,
    col_b integer NOT NULL,
    col_c integer NOT NULL,
    col_d integer NOT NULL,
    col_e nvarchar(100) NULL,
    col_f datetime NOT NULL DEFAULT GETDATE()
);

CREATE TABLE dbo.test_ic_uc
(
    id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
    col_a integer NOT NULL,
    col_b integer NOT NULL,
    col_c integer NOT NULL,
    col_d integer NOT NULL,
    col_e nvarchar(100) NULL
);

CREATE TABLE dbo.test_ic_filtered
(
    id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
    col_a integer NOT NULL,
    col_b integer NOT NULL,
    status_code integer NOT NULL
);

CREATE TABLE dbo.test_ic_heap
(
    col_a integer NOT NULL,
    col_b integer NOT NULL,
    col_c integer NOT NULL
);

CREATE TABLE dbo.test_ic_multi
(
    id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
    col_a integer NOT NULL,
    col_b integer NOT NULL
);

CREATE TABLE dbo.test_ic_view_base
(
    id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY CLUSTERED,
    col_a integer NOT NULL,
    col_b integer NOT NULL,
    col_c integer NOT NULL
);
GO

CREATE VIEW dbo.test_ic_view WITH SCHEMABINDING
AS
SELECT
    col_a = tvb.col_a,
    col_b = tvb.col_b,
    row_count = COUNT_BIG(*)
FROM dbo.test_ic_view_base AS tvb
GROUP BY tvb.col_a, tvb.col_b;
GO

/* Populate with 10K+ rows */
INSERT INTO dbo.test_ic_basic (col_a, col_b, col_c, col_d, col_e)
SELECT TOP (10000) ABS(CHECKSUM(NEWID())) % 1000, ABS(CHECKSUM(NEWID())) % 500,
    ABS(CHECKSUM(NEWID())) % 200, ABS(CHECKSUM(NEWID())) % 100, LEFT(NEWID(), 20)
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;

INSERT INTO dbo.test_ic_uc (col_a, col_b, col_c, col_d, col_e)
SELECT TOP (10000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    ABS(CHECKSUM(NEWID())) % 500, ABS(CHECKSUM(NEWID())) % 200,
    ABS(CHECKSUM(NEWID())) % 100, LEFT(NEWID(), 20)
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;

INSERT INTO dbo.test_ic_filtered (col_a, col_b, status_code)
SELECT TOP (10000) ABS(CHECKSUM(NEWID())) % 1000, ABS(CHECKSUM(NEWID())) % 500,
    ABS(CHECKSUM(NEWID())) % 5
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;

INSERT INTO dbo.test_ic_heap (col_a, col_b, col_c)
SELECT TOP (10000) ABS(CHECKSUM(NEWID())) % 1000, ABS(CHECKSUM(NEWID())) % 500,
    ABS(CHECKSUM(NEWID())) % 200
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;

INSERT INTO dbo.test_ic_multi (col_a, col_b)
SELECT TOP (10000) ABS(CHECKSUM(NEWID())) % 1000, ABS(CHECKSUM(NEWID())) % 500
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;

INSERT INTO dbo.test_ic_view_base (col_a, col_b, col_c)
SELECT TOP (10000) ABS(CHECKSUM(NEWID())) % 100, ABS(CHECKSUM(NEWID())) % 50,
    ABS(CHECKSUM(NEWID())) % 200
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;
GO

/* ============================================= */
/* Create test indexes                           */
/* ============================================= */

/* Group 1: UC as superset (#721, #724) */
ALTER TABLE dbo.test_ic_uc ADD CONSTRAINT uq_uc_abc UNIQUE (col_a, col_b, col_c);
CREATE NONCLUSTERED INDEX ix_uc_ab ON dbo.test_ic_uc (col_a, col_b);
CREATE NONCLUSTERED INDEX ix_uc_ab_inc ON dbo.test_ic_uc (col_a, col_b) INCLUDE (col_e);
CREATE NONCLUSTERED INDEX ix_uc_bc ON dbo.test_ic_uc (col_b, col_c);
CREATE UNIQUE NONCLUSTERED INDEX uix_uc_acd ON dbo.test_ic_uc (col_a, col_c, col_d);
CREATE NONCLUSTERED INDEX ix_uc_ac ON dbo.test_ic_uc (col_a, col_c);
ALTER TABLE dbo.test_ic_uc ADD CONSTRAINT uq_uc_ad UNIQUE (col_a, col_d);

/* Group 2: Sort direction */
CREATE INDEX ix_sort_a_desc ON dbo.test_ic_basic (col_a DESC);
CREATE INDEX ix_sort_a_desc2 ON dbo.test_ic_basic (col_a DESC);
CREATE INDEX ix_sort_a_asc ON dbo.test_ic_basic (col_a ASC);
CREATE INDEX ix_sort_ab_asc ON dbo.test_ic_basic (col_a ASC, col_b ASC);
CREATE INDEX ix_sort_ab_mixed ON dbo.test_ic_basic (col_a DESC, col_b ASC);

/* Group 3: Filtered indexes */
CREATE INDEX ix_filt_a_s1 ON dbo.test_ic_filtered (col_a) WHERE status_code = 1;
CREATE INDEX ix_filt_a_s1_dup ON dbo.test_ic_filtered (col_a) WHERE status_code = 1;
CREATE INDEX ix_filt_a_s2 ON dbo.test_ic_filtered (col_a) WHERE status_code = 2;
CREATE INDEX ix_filt_ab_s3 ON dbo.test_ic_filtered (col_a, col_b) WHERE status_code = 3;
CREATE INDEX ix_filt_a_s3 ON dbo.test_ic_filtered (col_a) WHERE status_code = 3;
CREATE INDEX ix_filt_ab_s4 ON dbo.test_ic_filtered (col_a, col_b) WHERE status_code = 4;
CREATE INDEX ix_filt_a_s0 ON dbo.test_ic_filtered (col_a) WHERE status_code = 0;

/* Group 4a: Key Duplicate — same keys, different includes, no wider index */
CREATE INDEX ix_inc_f_inc_b ON dbo.test_ic_basic (col_f) INCLUDE (col_b);
CREATE INDEX ix_inc_f_inc_c ON dbo.test_ic_basic (col_f) INCLUDE (col_c);

/* Group 4b: Key Subset — narrower key with includes absorbed by wider key */
CREATE INDEX ix_inc_cd_inc_e ON dbo.test_ic_basic (col_c, col_d) INCLUDE (col_e);
CREATE INDEX ix_inc_c_inc_b ON dbo.test_ic_basic (col_c) INCLUDE (col_b);

/* Group 5: Indexed view */
CREATE UNIQUE CLUSTERED INDEX cx_test_ic_view ON dbo.test_ic_view (col_a, col_b);
CREATE NONCLUSTERED INDEX ix_view_a ON dbo.test_ic_view (col_a);
CREATE NONCLUSTERED INDEX ix_view_a_dup ON dbo.test_ic_view (col_a);

/* Group 6: Heap */
CREATE NONCLUSTERED INDEX ix_heap_a ON dbo.test_ic_heap (col_a);
CREATE NONCLUSTERED INDEX ix_heap_a_dup ON dbo.test_ic_heap (col_a);

/* Group 7: Multi-table isolation */
CREATE INDEX ix_multi_a ON dbo.test_ic_multi (col_a);
CREATE INDEX ix_basic_col_d ON dbo.test_ic_basic (col_d);
GO

/* ============================================= */
/* Generate usage stats                          */
/* ============================================= */
DECLARE @c bigint, @i integer = 0;
WHILE @i < 10
BEGIN
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = uq_uc_abc) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = ix_uc_ab) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = ix_uc_ab_inc) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = ix_uc_bc) WHERE col_b = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = uix_uc_acd) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = ix_uc_ac) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_uc WITH (INDEX = uq_uc_ad) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_sort_a_desc) WHERE col_a > 500;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_sort_a_desc2) WHERE col_a > 600;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_sort_a_asc) WHERE col_a < 100;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_sort_ab_asc) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_sort_ab_mixed) WHERE col_a = 2;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_a_s1) WHERE col_a > 500 AND status_code = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_a_s1_dup) WHERE col_a > 600 AND status_code = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_a_s2) WHERE col_a > 500 AND status_code = 2;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_ab_s3) WHERE col_a = 1 AND status_code = 3;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_a_s3) WHERE col_a = 2 AND status_code = 3;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_ab_s4) WHERE col_a = 1 AND status_code = 4;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_filtered WITH (INDEX = ix_filt_a_s0) WHERE col_a = 1 AND status_code = 0;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_inc_f_inc_b) WHERE col_f > '2020-01-01';
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_inc_f_inc_c) WHERE col_f > '2021-01-01';
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_inc_cd_inc_e) WHERE col_c = 1 AND col_d = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_inc_c_inc_b) WHERE col_c = 2;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_view WITH (INDEX = ix_view_a, NOEXPAND) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_view WITH (INDEX = ix_view_a_dup, NOEXPAND) WHERE col_a = 2;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_heap WITH (INDEX = ix_heap_a) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_heap WITH (INDEX = ix_heap_a_dup) WHERE col_a = 2;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_multi WITH (INDEX = ix_multi_a) WHERE col_a = 1;
    SELECT @c = COUNT_BIG(*) FROM dbo.test_ic_basic WITH (INDEX = ix_basic_col_d) WHERE col_d = 1;
    SELECT @i += 1;
END;
GO

/* ============================================= */
/* Run sp_IndexCleanup                           */
/* ============================================= */
EXECUTE dbo.sp_IndexCleanup
    @database_name = N'StackOverflow2013',
    @dedupe_only = 1;
GO

/* ============================================= */
/* Cleanup                                       */
/* ============================================= */
IF OBJECT_ID('dbo.test_ic_view') IS NOT NULL
BEGIN
    DROP INDEX ix_view_a ON dbo.test_ic_view;
    DROP INDEX ix_view_a_dup ON dbo.test_ic_view;
    DROP INDEX cx_test_ic_view ON dbo.test_ic_view;
END;
GO
IF OBJECT_ID('dbo.test_ic_view') IS NOT NULL DROP VIEW dbo.test_ic_view;
GO
DROP TABLE IF EXISTS dbo.test_ic_basic;
DROP TABLE IF EXISTS dbo.test_ic_uc;
DROP TABLE IF EXISTS dbo.test_ic_filtered;
DROP TABLE IF EXISTS dbo.test_ic_heap;
DROP TABLE IF EXISTS dbo.test_ic_multi;
DROP TABLE IF EXISTS dbo.test_ic_view_base;
GO
