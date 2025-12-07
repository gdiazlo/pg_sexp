-- Benchmark: pg_sexp vs JSONB Performance Comparison
-- This script compares sexp performance against PostgreSQL's native JSONB

\timing on
\pset pager off

\echo ''
\echo '================================================================================'
\echo 'BENCHMARK: pg_sexp vs JSONB Comparison'
\echo '================================================================================'
\echo ''

-- Get PostgreSQL version
\echo 'PostgreSQL Version:'
SELECT version();

-- Clean up
DROP TABLE IF EXISTS bench_sexp CASCADE;
DROP TABLE IF EXISTS bench_jsonb CASCADE;

\echo ''
\echo '================================================================================'
\echo 'TEST 1: BULK INSERT PERFORMANCE (100,000 rows)'
\echo '================================================================================'

-- Create tables
CREATE TABLE bench_sexp (id serial PRIMARY KEY, data sexp);
CREATE TABLE bench_jsonb (id serial PRIMARY KEY, data jsonb);

\echo ''
\echo '--- Inserting 100,000 sexp rows ---'
\timing on
INSERT INTO bench_sexp (data)
SELECT (
    '(record ' || i || 
    ' (user (id ' || i || ') (name "user-' || i || '") (email "user' || i || '@example.com"))' ||
    ' (metadata (created ' || (1700000000 + i) || ') (modified ' || (1700000000 + i + 1000) || '))' ||
    ' (tags (' || 
        CASE (i % 5) 
            WHEN 0 THEN 'alpha beta gamma'
            WHEN 1 THEN 'delta epsilon'
            WHEN 2 THEN 'zeta eta theta'
            WHEN 3 THEN 'kappa lambda mu'
            ELSE 'omicron pi rho'
        END || '))' ||
    ' (settings (theme "' || (CASE (i % 3) WHEN 0 THEN 'dark' WHEN 1 THEN 'light' ELSE 'auto' END) || '"))' ||
    ')'
)::sexp
FROM generate_series(1, 100000) AS i;
\timing off

\echo ''
\echo '--- Inserting 100,000 jsonb rows ---'
\timing on
INSERT INTO bench_jsonb (data)
SELECT jsonb_build_object(
    'type', 'record',
    'id', i,
    'user', jsonb_build_object(
        'id', i,
        'name', 'user-' || i,
        'email', 'user' || i || '@example.com'
    ),
    'metadata', jsonb_build_object(
        'created', 1700000000 + i,
        'modified', 1700000000 + i + 1000
    ),
    'tags', CASE (i % 5) 
        WHEN 0 THEN '["alpha", "beta", "gamma"]'::jsonb
        WHEN 1 THEN '["delta", "epsilon"]'::jsonb
        WHEN 2 THEN '["zeta", "eta", "theta"]'::jsonb
        WHEN 3 THEN '["kappa", "lambda", "mu"]'::jsonb
        ELSE '["omicron", "pi", "rho"]'::jsonb
    END,
    'settings', jsonb_build_object(
        'theme', CASE (i % 3) WHEN 0 THEN 'dark' WHEN 1 THEN 'light' ELSE 'auto' END
    )
)
FROM generate_series(1, 100000) AS i;
\timing off

\echo ''
\echo '--- Storage comparison ---'
SELECT 
    'sexp' as type,
    pg_size_pretty(pg_total_relation_size('bench_sexp')) as total_size,
    pg_size_pretty(pg_relation_size('bench_sexp')) as table_size,
    (SELECT COUNT(*) FROM bench_sexp) as row_count
UNION ALL
SELECT 
    'jsonb' as type,
    pg_size_pretty(pg_total_relation_size('bench_jsonb')) as total_size,
    pg_size_pretty(pg_relation_size('bench_jsonb')) as table_size,
    (SELECT COUNT(*) FROM bench_jsonb) as row_count;

\echo ''
\echo '================================================================================'
\echo 'TEST 2: SEQUENTIAL SCAN PERFORMANCE'
\echo '================================================================================'

-- Warm up cache
SELECT COUNT(*) FROM bench_sexp;
SELECT COUNT(*) FROM bench_jsonb;

\echo ''
\echo '--- sexp: Full table scan with COUNT ---'
\timing on
SELECT COUNT(*) FROM bench_sexp;
\timing off

\echo ''
\echo '--- jsonb: Full table scan with COUNT ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb;
\timing off

\echo ''
\echo '--- sexp: Sum of serialized length ---'
\timing on
SELECT SUM(octet_length(data::text)) FROM bench_sexp;
\timing off

\echo ''
\echo '--- jsonb: Sum of serialized length ---'
\timing on
SELECT SUM(octet_length(data::text)) FROM bench_jsonb;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 3: ELEMENT ACCESS'
\echo '================================================================================'

\echo ''
\echo '--- sexp: car operation (get first element) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE car(data) = 'record'::sexp;
\timing off

\echo ''
\echo '--- jsonb: get type field ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data->>'type' = 'record';
\timing off

\echo ''
\echo '--- sexp: nth access (element 2 = user info) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE nth(data, 2) IS NOT NULL;
\timing off

\echo ''
\echo '--- jsonb: nested field access (user object) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data->'user' IS NOT NULL;
\timing off

\echo ''
\echo '--- sexp: sexp_length ---'
\timing on
SELECT AVG(sexp_length(data)) FROM bench_sexp;
\timing off

\echo ''
\echo '--- jsonb: jsonb_array_length (on tags) ---'
\timing on
SELECT AVG(jsonb_array_length(data->'tags')) FROM bench_jsonb;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 4: CONTAINMENT QUERIES (Sequential Scan)'
\echo '================================================================================'

\echo ''
\echo '--- sexp: Find symbol "alpha" anywhere (20% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> 'alpha'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Find "alpha" in tags array (20% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data->'tags' ? 'alpha';
\timing off

\echo ''
\echo '--- sexp: Find exact sublist (theme "dark") - 33% selectivity ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> '(theme "dark")'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Find theme = dark - 33% selectivity ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data @> '{"settings": {"theme": "dark"}}';
\timing off

\echo ''
\echo '--- sexp: Highly selective containment ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Highly selective containment ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data @> '{"user": {"id": 12345}}';
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 5: GIN INDEX PERFORMANCE'
\echo '================================================================================'

\echo ''
\echo '--- Create GIN indexes ---'
\timing on
CREATE INDEX bench_sexp_gin ON bench_sexp USING gin (data);
\timing off

\timing on
CREATE INDEX bench_jsonb_gin ON bench_jsonb USING gin (data);
\timing off

ANALYZE bench_sexp;
ANALYZE bench_jsonb;

\echo ''
\echo '--- sexp GIN: Find symbol "alpha" ---'
SET enable_seqscan = off;
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> 'alpha'::sexp;
\timing off

\echo ''
\echo '--- jsonb GIN: Find "alpha" in tags ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data @> '{"tags": ["alpha"]}';
\timing off

\echo ''
\echo '--- sexp GIN: Highly selective query ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo '--- jsonb GIN: Highly selective query ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data @> '{"user": {"id": 12345}}';
\timing off

SET enable_seqscan = on;

\echo ''
\echo '================================================================================'
\echo 'TEST 6: KEY-BASED CONTAINMENT (sexp only)'
\echo '================================================================================'

\echo ''
\echo '--- sexp @>> key-based containment (33% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @>> '(settings (theme "dark"))'::sexp;
\timing off

\echo ''
\echo '--- sexp @>> highly selective ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @>> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 7: PATTERN MATCHING (sexp only)'
\echo '================================================================================'

\echo ''
\echo '--- sexp pattern match (record _ _*) - all records ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data ~ '(record _ _*)'::sexp;
\timing off

\echo ''
\echo '--- sexp pattern match (record _ (user _*) _*) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data ~ '(record _ (user _*) _*)'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 8: sexp_find PERFORMANCE'
\echo '================================================================================'

\echo ''
\echo '--- sexp_find: Find (id _) pattern ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE sexp_find(data, '(id _)'::sexp) IS NOT NULL;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 9: HASH INDEX PERFORMANCE'
\echo '================================================================================'

DROP INDEX IF EXISTS bench_sexp_gin;
DROP INDEX IF EXISTS bench_jsonb_gin;

\echo ''
\echo '--- Create hash indexes ---'
\timing on
CREATE INDEX bench_sexp_hash ON bench_sexp USING hash (data);
\timing off

\timing on
CREATE INDEX bench_jsonb_hash ON bench_jsonb USING hash (data);
\timing off

ANALYZE bench_sexp;
ANALYZE bench_jsonb;

\echo ''
\echo '--- sexp hash equality lookup ---'
SET enable_seqscan = off;
\timing on
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, FORMAT TEXT)
SELECT * FROM bench_sexp 
WHERE data = '(record 12345 (user (id 12345) (name "user-12345") (email "user12345@example.com")) (metadata (created 1700012345) (modified 1700013345)) (tags (alpha beta gamma)) (settings (theme "dark")))'::sexp;
\timing off

\echo ''
\echo '--- jsonb hash equality lookup ---'
\timing on
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, FORMAT TEXT)
SELECT * FROM bench_jsonb
WHERE data = '{"type": "record", "id": 12345, "user": {"id": 12345, "name": "user-12345", "email": "user12345@example.com"}, "metadata": {"created": 1700012345, "modified": 1700013345}, "tags": ["alpha", "beta", "gamma"], "settings": {"theme": "dark"}}'::jsonb;
\timing off
SET enable_seqscan = on;

\echo ''
\echo '================================================================================'
\echo 'CLEANUP'
\echo '================================================================================'

DROP TABLE IF EXISTS bench_sexp CASCADE;
DROP TABLE IF EXISTS bench_jsonb CASCADE;

\echo ''
\echo '================================================================================'
\echo 'BENCHMARK COMPLETE'
\echo '================================================================================'
