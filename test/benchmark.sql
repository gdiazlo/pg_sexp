-- Benchmark: pg_sexp vs JSONB (Large Scale)
-- This script compares performance of s-expressions vs JSONB with realistic workloads
--
-- Key improvements over simple benchmarks:
-- 1. Larger datasets (500K-1M rows)
-- 2. More complex/realistic s-expressions (AST-like structures)
-- 3. Varied data sizes (small, medium, large expressions)
-- 4. Deep nesting tests
-- 5. Realistic query patterns

\timing on
\pset pager off

-- Ensure pg_sexp extension is available
CREATE EXTENSION IF NOT EXISTS pg_sexp;

-- Clean up any previous benchmark data
DROP TABLE IF EXISTS bench_sexp CASCADE;
DROP TABLE IF EXISTS bench_jsonb CASCADE;
DROP TABLE IF EXISTS bench_sexp_ast CASCADE;
DROP TABLE IF EXISTS bench_jsonb_ast CASCADE;
DROP TABLE IF EXISTS bench_sexp_deep CASCADE;
DROP TABLE IF EXISTS bench_jsonb_deep CASCADE;
DROP TABLE IF EXISTS bench_sexp_gin CASCADE;
DROP TABLE IF EXISTS bench_jsonb_gin CASCADE;

\echo ''
\echo '================================================================================'
\echo 'BENCHMARK: pg_sexp vs JSONB (Large Scale)'
\echo '================================================================================'
\echo ''

-- Get PostgreSQL version
\echo 'PostgreSQL Version:'
SELECT version();

\echo ''
\echo '================================================================================'
\echo 'TEST 1: BULK INSERT PERFORMANCE (500,000 rows)'
\echo '================================================================================'

CREATE TABLE bench_sexp (id serial PRIMARY KEY, data sexp);
CREATE TABLE bench_jsonb (id serial PRIMARY KEY, data jsonb);

\echo ''
\echo '--- sexp: Inserting 500,000 rows ---'
\timing on
INSERT INTO bench_sexp (data)
SELECT (
    '(record ' || i || 
    ' (user (id ' || i || ') (name "user-' || i || '") (email "user' || i || '@example.com"))' ||
    ' (metadata (created ' || (1700000000 + i) || ') (modified ' || (1700000000 + i + 1000) || ') (version ' || (i % 10) || '))' ||
    ' (tags (' || 
        CASE (i % 5) 
            WHEN 0 THEN 'alpha beta gamma'
            WHEN 1 THEN 'delta epsilon'
            WHEN 2 THEN 'zeta eta theta iota'
            WHEN 3 THEN 'kappa lambda mu nu xi'
            ELSE 'omicron pi rho sigma tau'
        END || '))' ||
    ' (settings (notifications ' || (CASE WHEN i % 2 = 0 THEN 'true' ELSE 'false' END) || ')' ||
              ' (theme "' || (CASE (i % 3) WHEN 0 THEN 'dark' WHEN 1 THEN 'light' ELSE 'auto' END) || '")' ||
              ' (language "' || (CASE (i % 4) WHEN 0 THEN 'en' WHEN 1 THEN 'es' WHEN 2 THEN 'fr' ELSE 'de' END) || '"))' ||
    ' (scores (' || (random() * 100)::int || ' ' || (random() * 100)::int || ' ' || (random() * 100)::int || ' ' || (random() * 100)::int || ' ' || (random() * 100)::int || '))' ||
    ')'
)::sexp
FROM generate_series(1, 500000) AS i;
\timing off

\echo ''
\echo '--- jsonb: Inserting 500,000 rows ---'
\timing on
INSERT INTO bench_jsonb (data)
SELECT (
    '{"id": ' || i || 
    ', "user": {"id": ' || i || ', "name": "user-' || i || '", "email": "user' || i || '@example.com"}' ||
    ', "metadata": {"created": ' || (1700000000 + i) || ', "modified": ' || (1700000000 + i + 1000) || ', "version": ' || (i % 10) || '}' ||
    ', "tags": [' || 
        CASE (i % 5) 
            WHEN 0 THEN '"alpha", "beta", "gamma"'
            WHEN 1 THEN '"delta", "epsilon"'
            WHEN 2 THEN '"zeta", "eta", "theta", "iota"'
            WHEN 3 THEN '"kappa", "lambda", "mu", "nu", "xi"'
            ELSE '"omicron", "pi", "rho", "sigma", "tau"'
        END || ']' ||
    ', "settings": {"notifications": ' || (CASE WHEN i % 2 = 0 THEN 'true' ELSE 'false' END) ||
                  ', "theme": "' || (CASE (i % 3) WHEN 0 THEN 'dark' WHEN 1 THEN 'light' ELSE 'auto' END) || '"' ||
                  ', "language": "' || (CASE (i % 4) WHEN 0 THEN 'en' WHEN 1 THEN 'es' WHEN 2 THEN 'fr' ELSE 'de' END) || '"}' ||
    ', "scores": [' || (random() * 100)::int || ', ' || (random() * 100)::int || ', ' || (random() * 100)::int || ', ' || (random() * 100)::int || ', ' || (random() * 100)::int || ']' ||
    '}'
)::jsonb
FROM generate_series(1, 500000) AS i;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 2: STORAGE SIZE COMPARISON'
\echo '================================================================================'

\echo ''
\echo '--- Table and index sizes ---'
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
\echo '--- Average row size (bytes) ---'
SELECT 
    'sexp' as type,
    pg_relation_size('bench_sexp') / COUNT(*) as avg_row_bytes
FROM bench_sexp
UNION ALL
SELECT 
    'jsonb' as type,
    pg_relation_size('bench_jsonb') / COUNT(*) as avg_row_bytes
FROM bench_jsonb;

\echo ''
\echo '================================================================================'
\echo 'TEST 3: SEQUENTIAL SCAN PERFORMANCE (500K rows)'
\echo '================================================================================'

-- Warm up the cache
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
\echo '--- sexp: Full table scan with SUM of data length ---'
\timing on
SELECT SUM(octet_length(data::text)) FROM bench_sexp;
\timing off

\echo ''
\echo '--- jsonb: Full table scan with SUM of data length ---'
\timing on
SELECT SUM(octet_length(data::text)) FROM bench_jsonb;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 4: ELEMENT ACCESS PATTERNS (500K rows)'
\echo '================================================================================'

\echo ''
\echo '--- sexp: Access first element (car) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE car(data) = 'record'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Access first-level key ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data ? 'id';
\timing off

\echo ''
\echo '--- sexp: Access nested element (nth 2 = user info) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE nth(data, 2) IS NOT NULL;
\timing off

\echo ''
\echo '--- jsonb: Access nested element (user object) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data->'user' IS NOT NULL;
\timing off

\echo ''
\echo '--- sexp: Deep nested access (user -> id) via car/cdr ---'
\timing on
SELECT COUNT(*) FROM bench_sexp 
WHERE car(nth(data, 2)) = 'user'::sexp 
  AND nth(nth(data, 2), 1) IS NOT NULL;
\timing off

\echo ''
\echo '--- jsonb: Deep nested access (user -> id) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb WHERE data->'user'->>'id' IS NOT NULL;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 5: KEY-BASED CONTAINMENT (500K rows) - Fair Comparison'
\echo '================================================================================'
\echo ''
\echo 'Both sexp @>> and jsonb @> use KEY-BASED containment semantics:'
\echo '  - Match by nested key/value pairs'
\echo '  - Order independent within objects/lists'
\echo '  - Partial matching (needle is subset of container)'

\echo ''
\echo '--- sexp @>>: Filter by settings.theme = dark (33% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp 
WHERE data @>> '(settings (theme "dark"))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: Filter by settings.theme = dark (33% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb 
WHERE data @> '{"settings": {"theme": "dark"}}';
\timing off

\echo ''
\echo '--- sexp @>>: Filter by user.id = 12345 (highly selective) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp 
WHERE data @>> '(user (id 12345))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: Filter by user.id = 12345 (highly selective) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb 
WHERE data @> '{"user": {"id": 12345}}';
\timing off

\echo ''
\echo '--- sexp @>>: Multi-condition (settings.theme=dark AND notifications=true) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp 
WHERE data @>> '(settings (theme "dark") (notifications true))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: Multi-condition (settings.theme=dark AND notifications=true) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb 
WHERE data @> '{"settings": {"theme": "dark", "notifications": true}}';
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 5b: STRUCTURAL CONTAINMENT (sexp @> only)'
\echo '================================================================================'
\echo ''
\echo 'sexp @> finds EXACT sublists anywhere in structure (unique to sexp)'

\echo ''
\echo '--- sexp @>: Find exact (theme "dark") sublist anywhere ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> '(theme "dark")'::sexp;
\timing off

\echo ''
\echo '--- sexp @>: Find symbol alpha anywhere ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> 'alpha'::sexp;
\timing off

\echo ''
\echo '--- sexp @>: Find string "dark" anywhere ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE data @> '"dark"'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 6: COMPLEX AST-LIKE STRUCTURES (200K rows)'
\echo '================================================================================'

CREATE TABLE bench_sexp_ast (id serial PRIMARY KEY, data sexp);
CREATE TABLE bench_jsonb_ast (id serial PRIMARY KEY, data jsonb);

\echo ''
\echo '--- sexp: Insert 200K AST-like expressions ---'
\timing on
INSERT INTO bench_sexp_ast (data)
SELECT (
    '(defun func-' || i || ' (x y z)' ||
    ' (let ((a (* x ' || (i % 100) || '))' ||
    '       (b (+ y ' || (i % 50) || '))' ||
    '       (c (- z ' || (i % 25) || ')))' ||
    '   (cond' ||
    '     ((< a 0) (error "negative"))' ||
    '     ((= a 0) nil)' ||
    '     ((> a ' || (i % 1000) || ') (progn (print "large") (* a b)))' ||
    '     (t (if (and (> b 0) (< c 100))' ||
    '            (+ a b c)' ||
    '            (- a b c))))))'
)::sexp
FROM generate_series(1, 200000) AS i;
\timing off

\echo ''
\echo '--- jsonb: Insert 200K AST-like structures ---'
\timing on
INSERT INTO bench_jsonb_ast (data)
SELECT (
    '{"type": "defun", "name": "func-' || i || '", "params": ["x", "y", "z"],' ||
    ' "body": {"type": "let", "bindings": [' ||
    '   {"name": "a", "value": {"op": "*", "args": ["x", ' || (i % 100) || ']}},' ||
    '   {"name": "b", "value": {"op": "+", "args": ["y", ' || (i % 50) || ']}},' ||
    '   {"name": "c", "value": {"op": "-", "args": ["z", ' || (i % 25) || ']}}],' ||
    ' "body": {"type": "cond", "clauses": [' ||
    '   {"test": {"op": "<", "args": ["a", 0]}, "body": {"type": "error", "msg": "negative"}},' ||
    '   {"test": {"op": "=", "args": ["a", 0]}, "body": null},' ||
    '   {"test": {"op": ">", "args": ["a", ' || (i % 1000) || ']}, "body": {"type": "progn", "forms": [{"type": "print", "value": "large"}, {"op": "*", "args": ["a", "b"]}]}},' ||
    '   {"test": true, "body": {"type": "if", "test": {"op": "and", "args": [{"op": ">", "args": ["b", 0]}, {"op": "<", "args": ["c", 100]}]},' ||
    '                           "then": {"op": "+", "args": ["a", "b", "c"]},' ||
    '                           "else": {"op": "-", "args": ["a", "b", "c"]}}}]}}}'
)::jsonb
FROM generate_series(1, 200000) AS i;
\timing off

\echo ''
\echo '--- AST storage sizes ---'
SELECT 
    'sexp_ast' as type,
    pg_size_pretty(pg_total_relation_size('bench_sexp_ast')) as total_size,
    pg_size_pretty(pg_relation_size('bench_sexp_ast')) as table_size
UNION ALL
SELECT 
    'jsonb_ast' as type,
    pg_size_pretty(pg_total_relation_size('bench_jsonb_ast')) as total_size,
    pg_size_pretty(pg_relation_size('bench_jsonb_ast')) as table_size;

\echo ''
\echo '--- sexp: Find all functions with "let" binding ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_ast WHERE data @> 'let'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Find all functions with "let" type ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_ast WHERE data->'body'->>'type' = 'let';
\timing off

\echo ''
\echo '--- sexp: Find functions with "cond" form ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_ast WHERE data @> 'cond'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Find functions with "cond" type ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_ast WHERE data::text LIKE '%"type": "cond"%';
\timing off

\echo ''
\echo '--- sexp: Find functions using multiplication (*) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_ast WHERE data @> '(*)'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Find functions using multiplication ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_ast WHERE data @> '{"op": "*"}';
\timing off

\echo ''
\echo '--- sexp: Count list operations (car) on AST ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_ast WHERE car(data) = 'defun'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Count type checks on AST ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_ast WHERE data->>'type' = 'defun';
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 7: DEEPLY NESTED STRUCTURES (100K rows, 15 levels)'
\echo '================================================================================'

CREATE TABLE bench_sexp_deep (id serial PRIMARY KEY, data sexp);
CREATE TABLE bench_jsonb_deep (id serial PRIMARY KEY, data jsonb);

\echo ''
\echo '--- sexp: Insert 100K deeply nested (15 levels) ---'
\timing on
INSERT INTO bench_sexp_deep (data)
SELECT (
    '(level1 ' || i || 
    ' (level2 (level3 (level4 (level5 ' ||
    '   (level6 (level7 (level8 (level9 (level10 ' ||
    '     (level11 (level12 (level13 (level14 (level15 ' ||
    '       (data (value ' || (i * 7 % 1000) || ') (name "item-' || i || '"))' ||
    '     )))))))))))))))'
)::sexp
FROM generate_series(1, 100000) AS i;
\timing off

\echo ''
\echo '--- jsonb: Insert 100K deeply nested (15 levels) ---'
\timing on
INSERT INTO bench_jsonb_deep (data)
SELECT (
    '{"level1": {"id": ' || i || 
    ', "level2": {"level3": {"level4": {"level5": ' ||
    '{"level6": {"level7": {"level8": {"level9": {"level10": ' ||
    '{"level11": {"level12": {"level13": {"level14": {"level15": ' ||
    '{"data": {"value": ' || (i * 7 % 1000) || ', "name": "item-' || i || '"}}' ||
    '}}}}}}}}}}}}}}}'
)::jsonb
FROM generate_series(1, 100000) AS i;
\timing off

\echo ''
\echo '--- Deep nesting storage sizes ---'
SELECT 
    'sexp_deep' as type,
    pg_size_pretty(pg_total_relation_size('bench_sexp_deep')) as total_size,
    pg_size_pretty(pg_relation_size('bench_sexp_deep')) as table_size
UNION ALL
SELECT 
    'jsonb_deep' as type,
    pg_size_pretty(pg_total_relation_size('bench_jsonb_deep')) as total_size,
    pg_size_pretty(pg_relation_size('bench_jsonb_deep')) as table_size;

\echo ''
\echo '--- sexp: Sequential scan of deeply nested data ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_deep;
\timing off

\echo ''
\echo '--- jsonb: Sequential scan of deeply nested data ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_deep;
\timing off

\echo ''
\echo '--- sexp: Find by deep containment ---'
\timing on
-- Search for exact (value 500) sublist - sexp uses structural containment
SELECT COUNT(*) FROM bench_sexp_deep WHERE data @> '(value 500)'::sexp;
\timing off

\echo ''
\echo '--- jsonb: Find by deep containment ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_deep WHERE data @> '{"level1":{"level2":{"level3":{"level4":{"level5":{"level6":{"level7":{"level8":{"level9":{"level10":{"level11":{"level12":{"level13":{"level14":{"level15":{"data":{"value":500}}}}}}}}}}}}}}}}}'::jsonb;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 8: GIN INDEX PERFORMANCE (500K rows)'
\echo '================================================================================'

CREATE TABLE bench_sexp_gin (id serial PRIMARY KEY, data sexp);
CREATE TABLE bench_jsonb_gin (id serial PRIMARY KEY, data jsonb);

\echo ''
\echo '--- Inserting 500K rows for GIN benchmark ---'
\timing on
INSERT INTO bench_sexp_gin (data)
SELECT (
    '(event ' || i || 
    ' (type "' || (CASE (i % 10) 
        WHEN 0 THEN 'click' WHEN 1 THEN 'view' WHEN 2 THEN 'purchase'
        WHEN 3 THEN 'login' WHEN 4 THEN 'logout' WHEN 5 THEN 'signup'
        WHEN 6 THEN 'error' WHEN 7 THEN 'warning' WHEN 8 THEN 'info'
        ELSE 'debug' END) || '")' ||
    ' (user ' || (i % 10000) || ')' ||
    ' (session "sess-' || (i % 50000) || '")' ||
    ' (timestamp ' || (1700000000 + i) || ')' ||
    ' (properties' ||
    '   (browser "' || (CASE (i % 4) WHEN 0 THEN 'chrome' WHEN 1 THEN 'firefox' WHEN 2 THEN 'safari' ELSE 'edge' END) || '")' ||
    '   (os "' || (CASE (i % 3) WHEN 0 THEN 'windows' WHEN 1 THEN 'macos' ELSE 'linux' END) || '")' ||
    '   (mobile ' || (CASE WHEN i % 5 = 0 THEN 'true' ELSE 'false' END) || ')' ||
    '   (country "' || (CASE (i % 6) WHEN 0 THEN 'US' WHEN 1 THEN 'UK' WHEN 2 THEN 'DE' WHEN 3 THEN 'FR' WHEN 4 THEN 'ES' ELSE 'IT' END) || '"))' ||
    ' (tags (' || (CASE (i % 7) 
        WHEN 0 THEN 'important urgent'
        WHEN 1 THEN 'review'
        WHEN 2 THEN 'automated'
        WHEN 3 THEN 'manual important'
        WHEN 4 THEN 'scheduled'
        WHEN 5 THEN 'retry failed'
        ELSE 'normal' END) || ')))'
)::sexp
FROM generate_series(1, 500000) AS i;
\timing off

\timing on
INSERT INTO bench_jsonb_gin (data)
SELECT (
    '{"event_id": ' || i || 
    ', "type": "' || (CASE (i % 10) 
        WHEN 0 THEN 'click' WHEN 1 THEN 'view' WHEN 2 THEN 'purchase'
        WHEN 3 THEN 'login' WHEN 4 THEN 'logout' WHEN 5 THEN 'signup'
        WHEN 6 THEN 'error' WHEN 7 THEN 'warning' WHEN 8 THEN 'info'
        ELSE 'debug' END) || '"' ||
    ', "user_id": ' || (i % 10000) ||
    ', "session": "sess-' || (i % 50000) || '"' ||
    ', "timestamp": ' || (1700000000 + i) ||
    ', "properties": {' ||
    '    "browser": "' || (CASE (i % 4) WHEN 0 THEN 'chrome' WHEN 1 THEN 'firefox' WHEN 2 THEN 'safari' ELSE 'edge' END) || '",' ||
    '    "os": "' || (CASE (i % 3) WHEN 0 THEN 'windows' WHEN 1 THEN 'macos' ELSE 'linux' END) || '",' ||
    '    "mobile": ' || (CASE WHEN i % 5 = 0 THEN 'true' ELSE 'false' END) || ',' ||
    '    "country": "' || (CASE (i % 6) WHEN 0 THEN 'US' WHEN 1 THEN 'UK' WHEN 2 THEN 'DE' WHEN 3 THEN 'FR' WHEN 4 THEN 'ES' ELSE 'IT' END) || '"}' ||
    ', "tags": [' || (CASE (i % 7) 
        WHEN 0 THEN '"important", "urgent"'
        WHEN 1 THEN '"review"'
        WHEN 2 THEN '"automated"'
        WHEN 3 THEN '"manual", "important"'
        WHEN 4 THEN '"scheduled"'
        WHEN 5 THEN '"retry", "failed"'
        ELSE '"normal"' END) || ']}'
)::jsonb
FROM generate_series(1, 500000) AS i;
\timing off

\echo ''
\echo '--- GIN table sizes (before index) ---'
SELECT 
    'sexp_gin' as type,
    pg_size_pretty(pg_total_relation_size('bench_sexp_gin')) as total_size,
    pg_size_pretty(pg_relation_size('bench_sexp_gin')) as table_size
UNION ALL
SELECT 
    'jsonb_gin' as type,
    pg_size_pretty(pg_total_relation_size('bench_jsonb_gin')) as total_size,
    pg_size_pretty(pg_relation_size('bench_jsonb_gin')) as table_size;

\echo ''
\echo '--- sexp @>>: Containment WITHOUT GIN (seq scan) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin WHERE data @>> '(event (type "click"))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: Containment WITHOUT GIN (seq scan) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_gin WHERE data @> '{"type": "click"}'::jsonb;
\timing off

\echo ''
\echo '--- Creating GIN indexes ---'
\timing on
CREATE INDEX bench_sexp_gin_idx ON bench_sexp_gin USING gin (data sexp_gin_ops);
\timing off

\timing on
CREATE INDEX bench_jsonb_gin_idx ON bench_jsonb_gin USING gin (data jsonb_path_ops);
\timing off

ANALYZE bench_sexp_gin;
ANALYZE bench_jsonb_gin;

\echo ''
\echo '--- GIN Index sizes ---'
SELECT 
    'sexp_gin' as type,
    pg_size_pretty(pg_relation_size('bench_sexp_gin_idx')) as index_size
UNION ALL
SELECT 
    'jsonb_gin' as type,
    pg_size_pretty(pg_relation_size('bench_jsonb_gin_idx')) as index_size;

SET enable_seqscan = off;

\echo ''
\echo '--- sexp @>>: GIN - Find click events (10% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin WHERE data @>> '(event (type "click"))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: GIN - Find click events (10% selectivity) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_gin WHERE data @> '{"type": "click"}'::jsonb;
\timing off

\echo ''
\echo '--- sexp @>>: GIN - Find chrome + windows ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin 
WHERE data @>> '(properties (browser "chrome") (os "windows"))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: GIN - Find chrome + windows ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_gin 
WHERE data @> '{"properties": {"browser": "chrome", "os": "windows"}}'::jsonb;
\timing off

\echo ''
\echo '--- sexp @>>: GIN - Find mobile=true events ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin WHERE data @>> '(properties (mobile true))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: GIN - Find mobile=true events ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_gin WHERE data @> '{"properties": {"mobile": true}}'::jsonb;
\timing off

\echo ''
\echo '--- sexp @>>: GIN - Find event by ID (highly selective) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin WHERE data @>> '(event 12345)'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: GIN - Find event by ID (highly selective) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_gin WHERE data @> '{"event_id": 12345}'::jsonb;
\timing off

\echo ''
\echo '--- sexp @>>: GIN - Complex query (type=purchase AND mobile=true) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin 
WHERE data @>> '(event (type "purchase") (properties (mobile true)))'::sexp;
\timing off

\echo ''
\echo '--- jsonb @>: GIN - Complex query (type=purchase AND mobile=true) ---'
\timing on
SELECT COUNT(*) FROM bench_jsonb_gin 
WHERE data @> '{"type": "purchase", "properties": {"mobile": true}}'::jsonb;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 8b: STRUCTURAL CONTAINMENT WITH GIN (sexp @> only)'
\echo '================================================================================'

\echo ''
\echo '--- sexp @>: GIN - Find by tag symbol "important" (structural) ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin WHERE data @> 'important'::sexp;
\timing off

\echo ''
\echo '--- sexp @>: GIN - Find exact (type "click") sublist ---'
\timing on
SELECT COUNT(*) FROM bench_sexp_gin WHERE data @> '(type "click")'::sexp;
\timing off

SET enable_seqscan = on;

\echo ''
\echo '================================================================================'
\echo 'TEST 9: SERIALIZATION/OUTPUT PERFORMANCE (500K rows)'
\echo '================================================================================'

\echo ''
\echo '--- sexp: Serialize all 500K rows to text ---'
\timing on
SELECT SUM(LENGTH(data::text)) FROM bench_sexp;
\timing off

\echo ''
\echo '--- jsonb: Serialize all 500K rows to text ---'
\timing on
SELECT SUM(LENGTH(data::text)) FROM bench_jsonb;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 10: LIST OPERATIONS ON LARGE DATA (sexp-specific)'
\echo '================================================================================'

\echo ''
\echo '--- sexp: car operation on 500K rows ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE car(data) = 'record'::sexp;
\timing off

\echo ''
\echo '--- sexp: cdr operation on 500K rows ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE cdr(data) IS NOT NULL;
\timing off

\echo ''
\echo '--- sexp: sexp_length on 500K rows ---'
\timing on
SELECT AVG(sexp_length(data)) FROM bench_sexp;
\timing off

\echo ''
\echo '--- sexp: nth access (element 3 = metadata) on 500K rows ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE nth(data, 3) IS NOT NULL;
\timing off

\echo ''
\echo '--- sexp: Deep nth access on 500K rows ---'
\timing on
SELECT COUNT(*) FROM bench_sexp WHERE car(nth(data, 4)) = 'settings'::sexp;
\timing off

\echo ''
\echo '================================================================================'
\echo 'TEST 11: AGGREGATION QUERIES'
\echo '================================================================================'

\echo ''
\echo '--- sexp: Group by first-level element (using car of nth) ---'
\timing on
SELECT car(nth(data, 2))::text as user_field, COUNT(*) 
FROM bench_sexp 
GROUP BY car(nth(data, 2))
LIMIT 5;
\timing off

\echo ''
\echo '--- jsonb: Group by first-level key ---'
\timing on
SELECT data->'user'->>'id' as user_id, COUNT(*) 
FROM bench_jsonb 
GROUP BY data->'user'->>'id'
LIMIT 5;
\timing off

\echo ''
\echo '================================================================================'
\echo 'CLEANUP'
\echo '================================================================================'

DROP TABLE IF EXISTS bench_sexp CASCADE;
DROP TABLE IF EXISTS bench_jsonb CASCADE;
DROP TABLE IF EXISTS bench_sexp_ast CASCADE;
DROP TABLE IF EXISTS bench_jsonb_ast CASCADE;
DROP TABLE IF EXISTS bench_sexp_deep CASCADE;
DROP TABLE IF EXISTS bench_jsonb_deep CASCADE;
DROP TABLE IF EXISTS bench_sexp_gin CASCADE;
DROP TABLE IF EXISTS bench_jsonb_gin CASCADE;

\echo ''
\echo '================================================================================'
\echo 'BENCHMARK COMPLETE'
\echo '================================================================================'
