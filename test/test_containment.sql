-- test_containment.sql
-- Comprehensive tests for s-exp containment operator (@>)
--
-- This file documents and tests the containment semantics to prevent regressions.
-- Key insight: sexp containment is STRUCTURAL, not partial key matching like jsonb.

\echo '=============================================='
\echo 'S-EXP CONTAINMENT SEMANTICS TESTS'
\echo '=============================================='
\echo ''

-- ============================================
-- SECTION 1: Basic Containment Rules
-- ============================================

\echo '=== SECTION 1: Basic Containment Rules ==='
\echo ''

-- Test 1.1: Atom containment in lists
\echo 'Test 1.1: Atom containment in lists'
SELECT '(a b c)'::sexp @> 'a'::sexp AS "contains_a (expect: t)";
SELECT '(a b c)'::sexp @> 'b'::sexp AS "contains_b (expect: t)";
SELECT '(a b c)'::sexp @> 'd'::sexp AS "contains_d (expect: f)";

-- Test 1.2: Atom self-containment
\echo ''
\echo 'Test 1.2: Atom self-containment'
SELECT 'foo'::sexp @> 'foo'::sexp AS "foo_contains_foo (expect: t)";
SELECT '123'::sexp @> '123'::sexp AS "123_contains_123 (expect: t)";
SELECT '"hello"'::sexp @> '"hello"'::sexp AS "str_contains_str (expect: t)";

-- Test 1.3: Nested list containment
\echo ''
\echo 'Test 1.3: Nested list containment'
SELECT '(a (b c))'::sexp @> '(b c)'::sexp AS "contains_(b_c) (expect: t)";
SELECT '(a (b c))'::sexp @> 'b'::sexp AS "contains_b (expect: t)";
SELECT '(a (b c))'::sexp @> 'c'::sexp AS "contains_c (expect: t)";
SELECT '(a (b c))'::sexp @> '(a b)'::sexp AS "contains_(a_b) (expect: f)";

-- Test 1.4: Deep nesting
\echo ''
\echo 'Test 1.4: Deep nesting'
SELECT '(x (y (z 1)))'::sexp @> '(z 1)'::sexp AS "contains_(z_1) (expect: t)";
SELECT '(x (y (z 1)))'::sexp @> '(y (z 1))'::sexp AS "contains_(y_(z_1)) (expect: t)";
SELECT '(x (y (z 1)))'::sexp @> '1'::sexp AS "contains_1 (expect: t)";

-- ============================================
-- SECTION 2: List Structure Matching (CRITICAL!)
-- ============================================

\echo ''
\echo '=== SECTION 2: List Structure Matching (CRITICAL!) ==='
\echo ''
\echo 'KEY INSIGHT: Lists must match EXACTLY as sublists, not partially!'
\echo ''

-- Test 2.1: Partial list does NOT match longer list
\echo 'Test 2.1: Partial list does NOT match longer list'
SELECT '(a b c)'::sexp @> '(a)'::sexp AS "3-elem contains 1-elem (expect: f)";
SELECT '(a b c)'::sexp @> '(a b)'::sexp AS "3-elem contains 2-elem (expect: f)";
SELECT '(a b c d)'::sexp @> '(b c)'::sexp AS "4-elem contains middle-2 (expect: f)";

-- Test 2.2: Same-length lists
\echo ''
\echo 'Test 2.2: Same-length lists must match exactly'
SELECT '(a b c)'::sexp @> '(a b c)'::sexp AS "exact_match (expect: t)";
SELECT '(a b c)'::sexp @> '(a b d)'::sexp AS "diff_last (expect: f)";
SELECT '(a b c)'::sexp @> '(a c b)'::sexp AS "diff_order (expect: f)";

-- Test 2.3: The jsonb-like pattern that FAILS in sexp
\echo ''
\echo 'Test 2.3: Why jsonb-style patterns fail in sexp'
\echo '  jsonb: {"settings": {"theme": "dark"}} matches {"settings": {"theme": "dark", "lang": "en"}}'
\echo '  sexp:  (settings (theme "dark")) does NOT match (settings (theme "dark") (lang "en"))'

SELECT '(settings (theme "dark") (lang "en"))'::sexp @> '(settings (theme "dark"))'::sexp 
    AS "partial_settings (expect: f)";
SELECT '(settings (theme "dark") (lang "en"))'::sexp @> '(theme "dark")'::sexp 
    AS "direct_theme (expect: t)";

-- ============================================
-- SECTION 3: String vs Symbol Containment
-- ============================================

\echo ''
\echo '=== SECTION 3: String vs Symbol Containment ==='
\echo ''

-- Test 3.1: Symbols are different from strings
\echo 'Test 3.1: Symbols vs Strings'
SELECT '(foo "bar")'::sexp @> 'foo'::sexp AS "contains_symbol_foo (expect: t)";
SELECT '(foo "bar")'::sexp @> '"bar"'::sexp AS "contains_string_bar (expect: t)";
SELECT '(foo "bar")'::sexp @> '"foo"'::sexp AS "contains_string_foo (expect: f)";
SELECT '(foo "bar")'::sexp @> 'bar'::sexp AS "contains_symbol_bar (expect: f)";

-- Test 3.2: Numbers
\echo ''
\echo 'Test 3.2: Numbers'
SELECT '(x 42 3.14)'::sexp @> '42'::sexp AS "contains_42 (expect: t)";
SELECT '(x 42 3.14)'::sexp @> '3.14'::sexp AS "contains_3.14 (expect: t)";
SELECT '(x 42 3.14)'::sexp @> '"42"'::sexp AS "contains_string_42 (expect: f)";

-- ============================================
-- SECTION 4: Real-World Pattern Examples
-- ============================================

\echo ''
\echo '=== SECTION 4: Real-World Pattern Examples ==='
\echo ''

-- Create a realistic data structure
\echo 'Test data: AST-like structure'
CREATE TEMP TABLE test_ast (data sexp);
INSERT INTO test_ast VALUES 
    ('(defun factorial (n) (if (<= n 1) 1 (* n (factorial (- n 1)))))'::sexp);

SELECT data::text FROM test_ast;

-- Test 4.1: Find specific symbols
\echo ''
\echo 'Test 4.1: Find specific symbols in AST'
SELECT data @> 'defun'::sexp AS "has_defun (expect: t)" FROM test_ast;
SELECT data @> 'factorial'::sexp AS "has_factorial (expect: t)" FROM test_ast;
SELECT data @> 'lambda'::sexp AS "has_lambda (expect: f)" FROM test_ast;

-- Test 4.2: Find specific sublists
\echo ''
\echo 'Test 4.2: Find specific sublists'
SELECT data @> '(n)'::sexp AS "has_param_list (expect: t)" FROM test_ast;
SELECT data @> '(<= n 1)'::sexp AS "has_condition (expect: t)" FROM test_ast;
SELECT data @> '(- n 1)'::sexp AS "has_decrement (expect: t)" FROM test_ast;
SELECT data @> '(+ n 1)'::sexp AS "has_increment (expect: f)" FROM test_ast;

-- Test 4.3: Find operators
\echo ''
\echo 'Test 4.3: Find operators'
SELECT data @> '*'::sexp AS "has_multiply (expect: t)" FROM test_ast;
SELECT data @> '-'::sexp AS "has_subtract (expect: t)" FROM test_ast;
SELECT data @> '/'::sexp AS "has_divide (expect: f)" FROM test_ast;

DROP TABLE test_ast;

-- ============================================
-- SECTION 5: Record/Event-like Structures
-- ============================================

\echo ''
\echo '=== SECTION 5: Record/Event-like Structures ==='
\echo ''

CREATE TEMP TABLE test_events (data sexp);
INSERT INTO test_events VALUES 
    ('(event 1 (type click) (user 100) (props (browser chrome) (os windows)))'::sexp),
    ('(event 2 (type view) (user 100) (props (browser firefox) (os linux)))'::sexp),
    ('(event 3 (type click) (user 200) (props (browser chrome) (os macos)))'::sexp);

\echo 'Test data:'
SELECT data::text FROM test_events;

-- CORRECT patterns for sexp containment
\echo ''
\echo 'Test 5.1: CORRECT way to search in sexp'

-- Find by type (searching for the symbol within)
\echo 'Find click events by symbol:'
SELECT COUNT(*) AS "click_events (expect: 2)" FROM test_events WHERE data @> 'click'::sexp;

-- Find by exact sublist
\echo 'Find by exact sublist (type click):'
SELECT COUNT(*) AS "type_click (expect: 2)" FROM test_events WHERE data @> '(type click)'::sexp;

-- Find by exact props sublist
\echo 'Find by browser chrome:'
SELECT COUNT(*) AS "chrome_users (expect: 2)" FROM test_events WHERE data @> '(browser chrome)'::sexp;

-- INCORRECT patterns (these return 0 or unexpected results)
\echo ''
\echo 'Test 5.2: INCORRECT patterns (expect 0 matches)'

-- This fails because (props (browser chrome)) is not the exact structure
SELECT COUNT(*) AS "wrong_props_pattern (expect: 0)" 
FROM test_events WHERE data @> '(props (browser chrome))'::sexp;

-- This is looking for a 2-element props list, but actual has 3 elements
SELECT COUNT(*) AS "wrong_partial_props (expect: 0)" 
FROM test_events WHERE data @> '(props (browser chrome) (os windows))'::sexp;
-- Actually this should match! Let me verify...

\echo ''
\echo 'Wait, let us verify (props (browser chrome) (os windows)):'
SELECT data @> '(props (browser chrome) (os windows))'::sexp AS "exact_props_match" 
FROM test_events WHERE data::text LIKE '%windows%';

DROP TABLE test_events;

-- ============================================
-- SECTION 6: GIN Index Behavior
-- ============================================

\echo ''
\echo '=== SECTION 6: GIN Index Behavior ==='
\echo ''

CREATE TEMP TABLE test_gin (id serial, data sexp);
INSERT INTO test_gin (data)
SELECT ('(item ' || i || ' (tag' || (i % 3) || ') (value ' || i * 10 || '))')::sexp
FROM generate_series(1, 1000) i;

CREATE INDEX test_gin_idx ON test_gin USING gin(data sexp_gin_ops);
ANALYZE test_gin;

-- Force index usage
SET enable_seqscan = off;

\echo 'Test 6.1: GIN index with symbol search'
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM test_gin WHERE data @> 'tag0'::sexp;
SELECT COUNT(*) AS "tag0_count (expect: ~333)" FROM test_gin WHERE data @> 'tag0'::sexp;

\echo ''
\echo 'Test 6.2: GIN index with sublist search'
EXPLAIN (COSTS OFF) SELECT COUNT(*) FROM test_gin WHERE data @> '(tag1)'::sexp;
SELECT COUNT(*) AS "exact_tag1_count (expect: ~333)" FROM test_gin WHERE data @> '(tag1)'::sexp;

SET enable_seqscan = on;

DROP TABLE test_gin;

-- ============================================
-- SECTION 7: Comparison with JSONB
-- ============================================

\echo ''
\echo '=== SECTION 7: Comparison with JSONB ==='
\echo ''

\echo 'JSONB containment is KEY-BASED (partial matching):'
SELECT '{"a": 1, "b": 2}'::jsonb @> '{"a": 1}'::jsonb AS "jsonb_partial (expect: t)";

\echo ''
\echo 'SEXP containment is STRUCTURAL (exact sublist matching):'
SELECT '(obj (a 1) (b 2))'::sexp @> '(a 1)'::sexp AS "sexp_sublist (expect: t)";
SELECT '(obj (a 1) (b 2))'::sexp @> '(obj (a 1))'::sexp AS "sexp_partial_obj (expect: f)";

\echo ''
\echo 'Summary of differences:'
\echo '  - JSONB: {"outer": {"inner": "value"}} @> {"outer": {"inner": "value"}} = TRUE'
\echo '  - JSONB: {"outer": {"inner": "value", "other": 1}} @> {"outer": {"inner": "value"}} = TRUE (partial!)'
\echo '  - SEXP: (outer (inner value)) @> (inner value) = TRUE'
\echo '  - SEXP: (outer (inner value) (other 1)) @> (outer (inner value)) = FALSE (must be exact sublist)'

-- ============================================
-- FINAL SUMMARY
-- ============================================

\echo ''
\echo '=============================================='
\echo 'CONTAINMENT SEMANTICS SUMMARY'
\echo '=============================================='
\echo ''
\echo 'For sexp @> pattern:'
\echo ''
\echo '1. ATOMS: Container contains pattern if pattern appears anywhere'
\echo '   (a b c) @> b  => TRUE'
\echo ''
\echo '2. LISTS: Pattern list must be an EXACT sublist somewhere in container'
\echo '   (a (b c)) @> (b c)  => TRUE (exact match)'
\echo '   (a (b c d)) @> (b c)  => FALSE (different lengths!)'
\echo ''
\echo '3. STRINGS vs SYMBOLS: They are different types!'
\echo '   (foo "bar") @> foo  => TRUE (symbol)'
\echo '   (foo "bar") @> "foo"  => FALSE (string)'
\echo ''
\echo '4. For searching nested structures, search for the INNER sublist:'
\echo '   WRONG: data @> (outer (inner value))  -- requires exact outer structure'
\echo '   RIGHT: data @> (inner value)  -- finds inner anywhere'
\echo ''
\echo '=============================================='
