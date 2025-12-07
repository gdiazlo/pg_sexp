-- Benchmark: C (pg_sexp) vs Rust (pg_sexp_rs) Implementation Comparison
-- This script compares the performance of both implementations

\timing on
\pset pager off

\echo ''
\echo '================================================================================'
\echo 'BENCHMARK: C vs Rust Implementation Comparison'
\echo '================================================================================'
\echo ''

-- Get PostgreSQL version
\echo 'PostgreSQL Version:'
SELECT version();

-- Clean up
DROP TABLE IF EXISTS bench_data CASCADE;

\echo ''
\echo '================================================================================'
\echo 'TEST 1: BULK INSERT PERFORMANCE (100,000 rows)'
\echo '================================================================================'

CREATE TABLE bench_data (id serial PRIMARY KEY, data sexp);

\echo ''
\echo '--- Inserting 100,000 rows ---'
\timing on
INSERT INTO bench_data (data)
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
\echo '--- Table size ---'
SELECT 
    pg_size_pretty(pg_total_relation_size('bench_data')) as total_size,
    pg_size_pretty(pg_relation_size('bench_data')) as table_size,
    (SELECT COUNT(*) FROM bench_data) as row_count;

\echo ''
\echo '================================================================================'
\echo 'TEST 2: SEQUENTIAL SCAN PERFORMANCE'
\echo '================================================================================'

-- Warm up cache
SELECT COUNT(*) FROM bench_data;

\echo ''
\echo '--- Full table scan with COUNT ---'
\timing on
SELECT COUNT(*) FROM bench_data;
\timing off

\echo ''
\echo '--- Sum of data length (serialization) ---'
\timing on
SELECT SUM(octet_length(data::text)) FROM bench_data;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 3: ELEMENT ACCESS (car, cdr, nth)'
\echo '================================================================================'

\echo ''
\echo '--- car operation ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE car(data) = 'record'::sexp;
\timing off

\echo ''
\echo '--- cdr operation ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE cdr(data) IS NOT NULL;
\timing off

\echo ''
\echo '--- nth access (element 2 = user info) ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE nth(data, 2) IS NOT NULL;
\timing off

\echo ''
\echo '--- sexp_length ---'
\timing on
SELECT AVG(sexp_length(data)) FROM bench_data;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 4: STRUCTURAL CONTAINMENT (@>)'
\echo '================================================================================'

\echo ''
\echo '--- Find symbol "alpha" anywhere (20% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data @> 'alpha'::sexp;
\timing off

\echo ''
\echo '--- Find exact sublist (theme "dark") ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data @> '(theme "dark")'::sexp;
\timing off

\echo ''
\echo '--- Find (user (id 12345)) - highly selective ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data @> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 5: KEY-BASED CONTAINMENT (@>>)'
\echo '================================================================================'

\echo ''
\echo '--- Find by (theme "dark") key-value (33% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data @>> '(settings (theme "dark"))'::sexp;
\timing off

\echo ''
\echo '--- Find by (user (id 12345)) key-value - highly selective ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data @>> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 6: PATTERN MATCHING (~)'
\echo '================================================================================'

\echo ''
\echo '--- Match pattern (record _ _*) - all records ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data ~ '(record _ _*)'::sexp;
\timing off

\echo ''
\echo '--- Match pattern (record _ (user _*) _*) ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE data ~ '(record _ (user _*) _*)'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 7: TYPE CHECKING'
\echo '================================================================================'

\echo ''
\echo '--- is_list checks ---'
\timing on
SELECT COUNT(*) FROM bench_data WHERE is_list(data);
\timing off

\echo ''
\echo '--- sexp_typeof ---'
\timing on
SELECT sexp_typeof(data), COUNT(*) 
FROM bench_data 
GROUP BY sexp_typeof(data);
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 8: HASH INDEX PERFORMANCE'
\echo '================================================================================'

\echo ''
\echo '--- Create hash index ---'
\timing on
CREATE INDEX bench_hash_idx ON bench_data USING hash (data);
\timing off

ANALYZE bench_data;

\echo ''
\echo '--- Hash index equality lookup ---'
SET enable_seqscan = off;
\timing on
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON) 
SELECT * FROM bench_data 
WHERE data = '(record 12345 (user (id 12345) (name "user-12345") (email "user12345@example.com")) (metadata (created 1700012345) (modified 1700013345)) (tags (alpha beta gamma)) (settings (theme "dark")))'::sexp;
\timing off
SET enable_seqscan = on;

\echo ''
\echo '================================================================================'
\echo 'TEST 9: COMPLEX NESTED ACCESS'
\echo '================================================================================'

\echo ''
\echo '--- Deep nested access via car/nth chain ---'
\timing on
SELECT COUNT(*) FROM bench_data 
WHERE car(nth(data, 2)) = 'user'::sexp;
\timing off

\echo ''
\echo '--- Double nested access ---'
\timing on
SELECT COUNT(*) FROM bench_data 
WHERE car(nth(nth(data, 2), 1)) = 'id'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'CLEANUP'
\echo '================================================================================'

DROP TABLE IF EXISTS bench_data CASCADE;

\echo ''
\echo '================================================================================'
\echo 'BENCHMARK COMPLETE'
\echo '================================================================================'
