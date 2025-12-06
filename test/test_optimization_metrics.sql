-- test_optimization_metrics.sql
--
-- Comprehensive tests for pg_sexp optimization effectiveness:
-- 1. Bloom filter hit rate (negative case rejection)
-- 2. Hash index correctness with different symbol-table orders
-- 3. GIN recheck rate measurement
--
-- Run with: psql -U postgres -d postgres -f test_optimization_metrics.sql

\timing on
\pset pager off

CREATE EXTENSION IF NOT EXISTS pg_sexp;

\echo ''
\echo '================================================================================'
\echo 'OPTIMIZATION METRICS TEST SUITE'
\echo '================================================================================'
\echo ''

-- =============================================================================
-- TEST 1: BLOOM FILTER REJECTION RATE FOR CONTAINMENT NEGATIVE CASES
-- =============================================================================
-- The Bloom signature should quickly reject non-matching containment queries
-- without traversing the entire structure.

\echo '================================================================================'
\echo 'TEST 1: BLOOM FILTER - NEGATIVE CASE REJECTION'
\echo '================================================================================'
\echo ''
\echo 'Purpose: Measure CPU time reduction for containment queries that SHOULD NOT match.'
\echo 'With effective Bloom filtering, negative cases should be much faster because'
\echo 'we reject early without full structural comparison.'
\echo ''

DROP TABLE IF EXISTS bloom_test CASCADE;
CREATE TABLE bloom_test (id serial PRIMARY KEY, data sexp);

-- Insert diverse structures with different Bloom signatures
\echo '--- Inserting 50,000 diverse structures ---'
INSERT INTO bloom_test (data)
SELECT CASE (i % 10)
    -- Type 0: (config (setting1 val1) (setting2 val2) (setting3 val3))
    WHEN 0 THEN ('(config (setting1 ' || i || ') (setting2 ' || (i*2) || ') (setting3 ' || (i*3) || '))')::sexp
    -- Type 1: (user (id N) (name "userN") (email "..."))
    WHEN 1 THEN ('(user (id ' || i || ') (name "user' || i || '") (email "user' || i || '@test.com"))')::sexp
    -- Type 2: (event (type "click") (timestamp N) (data (x 1) (y 2)))
    WHEN 2 THEN ('(event (type "click") (timestamp ' || i || ') (data (x ' || (i%100) || ') (y ' || (i%200) || ')))')::sexp
    -- Type 3: (log (level "info") (message "msg") (context (file "test.c") (line N)))
    WHEN 3 THEN ('(log (level "info") (message "log' || i || '") (context (file "test.c") (line ' || (i%1000) || ')))')::sexp
    -- Type 4: (ast (defun (name fn) (params (x y z)) (body (* x y))))
    WHEN 4 THEN ('(ast (defun (name fn' || i || ') (params (x y z)) (body (* x y))))')::sexp
    -- Type 5: (product (sku "SKU") (price N) (stock N) (tags (electronics sale)))
    WHEN 5 THEN ('(product (sku "SKU' || i || '") (price ' || (i%10000) || ') (stock ' || (i%500) || ') (tags (electronics sale)))')::sexp
    -- Type 6: (order (id N) (items ((item1 1) (item2 2))) (total N))
    WHEN 6 THEN ('(order (id ' || i || ') (items ((item1 1) (item2 2))) (total ' || (i*100) || '))')::sexp
    -- Type 7: (metrics (cpu N) (memory N) (disk N) (network N))
    WHEN 7 THEN ('(metrics (cpu ' || (i%100) || ') (memory ' || (i%100) || ') (disk ' || (i%100) || ') (network ' || (i%100) || '))')::sexp
    -- Type 8: (tree (node (left (leaf 1)) (right (leaf 2))))
    WHEN 8 THEN ('(tree (node (left (leaf ' || i || ')) (right (leaf ' || (i+1) || '))))')::sexp
    -- Type 9: (document (title "doc") (sections ((section1 "a") (section2 "b"))))
    WHEN 9 THEN ('(document (title "doc' || i || '") (sections ((section1 "a") (section2 "b"))))')::sexp
END
FROM generate_series(1, 50000) AS i;

ANALYZE bloom_test;

\echo ''
\echo '--- Test 1.1: POSITIVE matches (containment SHOULD succeed) ---'
\echo 'These queries find actual matches - baseline for comparison'
\echo ''

\echo 'Positive: Find all config entries (5000 rows, 10%)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> 'config'::sexp;
\timing off

\echo ''
\echo 'Positive: Find all users (5000 rows, 10%)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> 'user'::sexp;
\timing off

\echo ''
\echo 'Positive: Find events with click type (5000 rows, 10%)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(type "click")'::sexp;
\timing off

\echo ''
\echo '--- Test 1.2: NEGATIVE matches (containment SHOULD FAIL) ---'
\echo 'These queries find NO matches - Bloom filter should reject quickly'
\echo ''

\echo 'Negative: Search for nonexistent symbol "foobar" (0 rows expected)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> 'foobar'::sexp;
\timing off

\echo ''
\echo 'Negative: Search for nonexistent sublist (xyz 123) (0 rows expected)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(xyz 123)'::sexp;
\timing off

\echo ''
\echo 'Negative: Search for nonexistent nested (nonexistent (deep (value))) (0 rows expected)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(nonexistent (deep (value)))'::sexp;
\timing off

\echo ''
\echo 'Negative: Search for symbol that exists but with wrong sublist (config (xyz 1)) (0 rows)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(config (nonexistent 1))'::sexp;
\timing off

\echo ''
\echo 'Negative: Wrong type combination - user with event fields (0 rows expected)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(user (timestamp 1))'::sexp;
\timing off

\echo ''
\echo '--- Test 1.3: HIGHLY SELECTIVE matches (few rows match) ---'
\echo 'With Bloom, we should quickly skip non-matching rows'
\echo ''

\echo 'Selective: Find specific user ID (1 row expected)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo 'Selective: Find config with specific setting1 value (1 row expected)'
\timing on
SELECT COUNT(*) FROM bloom_test WHERE data @> '(config (setting1 10000))'::sexp;
\timing off

DROP TABLE bloom_test;

-- =============================================================================
-- TEST 2: HASH INDEX CORRECTNESS WITH DIFFERENT SYMBOL-TABLE ORDERS  
-- =============================================================================
-- sexp_hash must produce the same hash for semantically equal values,
-- even if their binary representations differ due to different symbol tables.

\echo ''
\echo '================================================================================'
\echo 'TEST 2: HASH INDEX CORRECTNESS - SYMBOL TABLE ORDER INVARIANCE'
\echo '================================================================================'
\echo ''
\echo 'Purpose: Verify sexp_hash produces identical hashes for semantically equal'
\echo 'values even when their internal symbol tables differ.'
\echo ''
\echo 'This is critical for hash index and hash join correctness.'
\echo ''

DROP TABLE IF EXISTS hash_test CASCADE;
CREATE TABLE hash_test (
    id serial PRIMARY KEY, 
    description text,
    expr1 sexp, 
    expr2 sexp
);

-- Test various ways to create semantically equal sexps with different symbol tables
\echo '--- Inserting test cases with different symbol-table construction orders ---'

-- Case 1: Same list, created directly vs from car extraction
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Direct symbol vs car() extraction',
     'a'::sexp, 
     car('(a b c)'::sexp));

-- Case 2: Integer in different contexts
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Integer alone vs extracted from list',
     '42'::sexp,
     car('(42 foo bar)'::sexp));

-- Case 3: String in different contexts
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('String alone vs extracted',
     '"hello"'::sexp,
     car('("hello" "world")'::sexp));

-- Case 4: Complex list - same structure but built differently
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('List built different ways',
     '(a b c)'::sexp,
     cdr('(x a b c)'::sexp));

-- Case 5: Nested structure
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Nested list extraction',
     '(inner val)'::sexp,
     nth('(outer (inner val) more)'::sexp, 1));

-- Case 6: Float values
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Float value extraction',
     '3.14'::sexp,
     car('(3.14 data)'::sexp));

-- Case 7: NIL handling
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('NIL equality',
     '()'::sexp,
     '()'::sexp);

-- Case 8: Different symbol tables for same symbols
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Symbols from different parent structures',
     car('(define x 10)'::sexp),
     car('(define (fn) body)'::sexp));

-- Case 9: Multi-symbol consistency
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Multi-element list from different sources',
     '(x 10)'::sexp,
     cdr('(define x 10)'::sexp));

-- Case 10: Deeply nested extraction
INSERT INTO hash_test (description, expr1, expr2) VALUES
    ('Deep nesting extraction',
     'target'::sexp,
     car(car(car('(((target))))'::sexp))));

\echo ''
\echo '--- Verifying semantic equality (expr1 = expr2) ---'
SELECT 
    id,
    description,
    expr1::text as expr1_text,
    expr2::text as expr2_text,
    (expr1 = expr2) as equal,
    CASE WHEN expr1 = expr2 THEN 'PASS' ELSE 'FAIL' END as status
FROM hash_test
ORDER BY id;

\echo ''
\echo '--- Verifying hash equality for equal values ---'
SELECT 
    id,
    description,
    sexp_hash(expr1) as hash1,
    sexp_hash(expr2) as hash2,
    (sexp_hash(expr1) = sexp_hash(expr2)) as hash_equal,
    CASE WHEN sexp_hash(expr1) = sexp_hash(expr2) THEN 'PASS' ELSE 'FAIL - HASH MISMATCH!' END as status
FROM hash_test
WHERE expr1 = expr2
ORDER BY id;

\echo ''
\echo '--- Summary: All equal pairs should have equal hashes ---'
SELECT 
    COUNT(*) as total_pairs,
    SUM(CASE WHEN expr1 = expr2 AND sexp_hash(expr1) = sexp_hash(expr2) THEN 1 ELSE 0 END) as correct,
    SUM(CASE WHEN expr1 = expr2 AND sexp_hash(expr1) != sexp_hash(expr2) THEN 1 ELSE 0 END) as hash_mismatches
FROM hash_test;

\echo ''
\echo '--- Test hash index with semantically equal lookups ---'
CREATE INDEX hash_test_idx ON hash_test USING hash (expr1);
ANALYZE hash_test;

\echo 'Hash index lookup for car(list) - should find row with direct symbol'
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) 
SELECT * FROM hash_test WHERE expr1 = car('(a b c)'::sexp);

SELECT id, description FROM hash_test WHERE expr1 = car('(a b c)'::sexp);

\echo ''
\echo 'Hash join test - joining on semantically equal values'
-- Create a second table with values that should join
DROP TABLE IF EXISTS hash_join_test;
CREATE TABLE hash_join_test (id serial PRIMARY KEY, val sexp);
INSERT INTO hash_join_test (val) VALUES 
    ('a'::sexp),
    ('42'::sexp),
    ('"hello"'::sexp),
    ('(a b c)'::sexp);

CREATE INDEX hash_join_idx ON hash_join_test USING hash (val);
ANALYZE hash_join_test;

\echo ''
\echo 'Hash join should match all expr1 values with their counterparts'
SET enable_hashjoin = on;
SET enable_mergejoin = off;
SET enable_nestloop = off;

EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
SELECT ht.description, hjt.val::text
FROM hash_test ht
JOIN hash_join_test hjt ON ht.expr1 = hjt.val;

SELECT ht.description, hjt.val::text
FROM hash_test ht
JOIN hash_join_test hjt ON ht.expr1 = hjt.val
ORDER BY ht.id;

RESET enable_hashjoin;
RESET enable_mergejoin;
RESET enable_nestloop;

DROP TABLE hash_join_test;
DROP TABLE hash_test;

-- =============================================================================
-- TEST 3: GIN INDEX RECHECK RATE
-- =============================================================================
-- With better GIN keys (Bloom signatures, pair keys), we should see:
-- - More precise index matches
-- - Fewer rechecks required
-- - Faster @> / @>> queries

\echo ''
\echo '================================================================================'
\echo 'TEST 3: GIN INDEX RECHECK RATE MEASUREMENT'
\echo '================================================================================'
\echo ''
\echo 'Purpose: Measure GIN index precision via recheck rates.'
\echo 'Better keys = fewer rows fetched from heap = fewer rechecks = faster queries.'
\echo ''

DROP TABLE IF EXISTS gin_recheck_test CASCADE;
CREATE TABLE gin_recheck_test (id serial PRIMARY KEY, data sexp);

-- Insert data with varying selectivity patterns
\echo '--- Inserting 100,000 rows with varied key distributions ---'
INSERT INTO gin_recheck_test (data)
SELECT (
    '(record ' || i ||
    ' (type "' || (CASE (i % 20) 
        WHEN 0 THEN 'alpha' WHEN 1 THEN 'beta' WHEN 2 THEN 'gamma' 
        WHEN 3 THEN 'delta' WHEN 4 THEN 'epsilon' WHEN 5 THEN 'zeta'
        WHEN 6 THEN 'eta' WHEN 7 THEN 'theta' WHEN 8 THEN 'iota'
        WHEN 9 THEN 'kappa' WHEN 10 THEN 'lambda' WHEN 11 THEN 'mu'
        WHEN 12 THEN 'nu' WHEN 13 THEN 'xi' WHEN 14 THEN 'omicron'
        WHEN 15 THEN 'pi' WHEN 16 THEN 'rho' WHEN 17 THEN 'sigma'
        WHEN 18 THEN 'tau' ELSE 'upsilon' END) || '")' ||
    ' (category ' || (i % 100) || ')' ||
    ' (status "' || (CASE (i % 5) WHEN 0 THEN 'active' WHEN 1 THEN 'pending' 
                     WHEN 2 THEN 'inactive' WHEN 3 THEN 'archived' ELSE 'deleted' END) || '")' ||
    ' (priority ' || (i % 10) || ')' ||
    ' (tags (' || (CASE (i % 7)
        WHEN 0 THEN 'important urgent'
        WHEN 1 THEN 'normal'
        WHEN 2 THEN 'review needed'
        WHEN 3 THEN 'automated'
        WHEN 4 THEN 'manual'
        WHEN 5 THEN 'scheduled recurring'
        ELSE 'misc' END) || '))' ||
    ' (metadata (created ' || (1700000000 + i) || ') (version ' || (i % 100) || '))' ||
    ')'
)::sexp
FROM generate_series(1, 100000) AS i;

\echo ''
\echo '--- Creating GIN index ---'
\timing on
CREATE INDEX gin_recheck_idx ON gin_recheck_test USING gin (data sexp_gin_ops);
\timing off

ANALYZE gin_recheck_test;

SET enable_seqscan = off;

\echo ''
\echo '=== GIN Recheck Analysis ===' 
\echo ''
\echo 'For each query, we show:'
\echo '  - Rows Removed by Index Recheck: rows fetched but filtered out'
\echo '  - Heap Blocks: pages accessed'
\echo '  - Lower recheck = better key precision'
\echo ''

\echo '--- Query 3.1: Single atom containment (type alpha, ~5% selectivity) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test WHERE data @> '(type "alpha")'::sexp;

SELECT COUNT(*) FROM gin_recheck_test WHERE data @> '(type "alpha")'::sexp;

\echo ''
\echo '--- Query 3.2: Single symbol containment (important, ~14% selectivity) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test WHERE data @> 'important'::sexp;

SELECT COUNT(*) FROM gin_recheck_test WHERE data @> 'important'::sexp;

\echo ''
\echo '--- Query 3.3: Key-based containment @>> (status active, 20% selectivity) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test WHERE data @>> '(record (status "active"))'::sexp;

SELECT COUNT(*) FROM gin_recheck_test WHERE data @>> '(record (status "active"))'::sexp;

\echo ''
\echo '--- Query 3.4: Multi-key containment (category + priority) ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test 
WHERE data @>> '(record (category 50) (priority 5))'::sexp;

SELECT COUNT(*) FROM gin_recheck_test 
WHERE data @>> '(record (category 50) (priority 5))'::sexp;

\echo ''
\echo '--- Query 3.5: Highly selective - specific record ID ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test WHERE data @>> '(record 50000)'::sexp;

SELECT COUNT(*) FROM gin_recheck_test WHERE data @>> '(record 50000)'::sexp;

\echo ''
\echo '--- Query 3.6: No matches - should use Bloom to reject quickly ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test WHERE data @> 'nonexistent_symbol_xyz'::sexp;

SELECT COUNT(*) FROM gin_recheck_test WHERE data @> 'nonexistent_symbol_xyz'::sexp;

\echo ''
\echo '--- Query 3.7: Sublist match within nested structure ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test WHERE data @> '(version 50)'::sexp;

SELECT COUNT(*) FROM gin_recheck_test WHERE data @> '(version 50)'::sexp;

\echo ''
\echo '--- Query 3.8: Complex multi-condition ---'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT COUNT(*) FROM gin_recheck_test 
WHERE data @>> '(record (type "alpha") (status "active") (priority 0))'::sexp;

SELECT COUNT(*) FROM gin_recheck_test 
WHERE data @>> '(record (type "alpha") (status "active") (priority 0))'::sexp;

RESET enable_seqscan;

\echo ''
\echo '--- GIN Index Statistics ---'
SELECT 
    relname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE relname = 'gin_recheck_test';

DROP TABLE gin_recheck_test;

-- =============================================================================
-- SUMMARY
-- =============================================================================

\echo ''
\echo '================================================================================'
\echo 'TEST SUMMARY'
\echo '================================================================================'
\echo ''
\echo 'TEST 1 - Bloom Filter Rejection:'
\echo '  - Negative containment queries should be fast (early rejection)'
\echo '  - Compare positive vs negative query times'
\echo '  - Lower times for negative = Bloom filter working'
\echo ''
\echo 'TEST 2 - Hash Correctness:'
\echo '  - All PASS = sexp_hash correctly handles symbol table variations'
\echo '  - Any FAIL = potential hash index corruption'
\echo ''
\echo 'TEST 3 - GIN Recheck Rate:'
\echo '  - "Rows Removed by Index Recheck" shows false positives'
\echo '  - Lower numbers = better key precision'
\echo '  - 0 rechecks = perfect index precision'
\echo ''
\echo '================================================================================'
