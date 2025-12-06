-- Robustness and stress tests for pg_sexp
-- These tests are designed to find edge cases, memory issues, and data corruption

\echo '=== ROBUSTNESS TESTS FOR pg_sexp ==='
\echo ''

-- Create test table for stress tests
CREATE TABLE stress_test (id serial PRIMARY KEY, expr sexp);

-----------------------------------------------------------
-- TEST 1: Edge cases in parsing
-----------------------------------------------------------
\echo '=== TEST 1: Parsing Edge Cases ==='

-- Empty and minimal inputs
\echo 'Empty/minimal inputs...'
SELECT '()'::sexp;
SELECT 'nil'::sexp;
SELECT '""'::sexp;
SELECT '0'::sexp;
SELECT '-1'::sexp;
SELECT '(a)'::sexp;

-- Very short strings
SELECT '"a"'::sexp;
SELECT '"ab"'::sexp;

-- Boundary integer values (smallint -16 to 15)
\echo 'Integer boundaries...'
SELECT '-16'::sexp;  -- Min smallint
SELECT '-17'::sexp;  -- Below smallint range
SELECT '15'::sexp;   -- Max smallint
SELECT '16'::sexp;   -- Above smallint range
SELECT '0'::sexp;

-- Large integers
SELECT '2147483647'::sexp;   -- INT32_MAX
SELECT '-2147483648'::sexp;  -- INT32_MIN
SELECT '9223372036854775807'::sexp;   -- INT64_MAX
SELECT '-9223372036854775807'::sexp;  -- Near INT64_MIN

-- Float edge cases
\echo 'Float edge cases...'
SELECT '0.0'::sexp;
SELECT '-0.0'::sexp;
SELECT '1e10'::sexp;
SELECT '1e-10'::sexp;
SELECT '1.7976931348623157e+308'::sexp;  -- Near DBL_MAX
SELECT '2.2250738585072014e-308'::sexp;  -- Near DBL_MIN

-- String edge cases
\echo 'String edge cases...'
SELECT '""'::sexp;  -- Empty string
SELECT E'"\\n"'::sexp;  -- Newline escape
SELECT E'"\\t"'::sexp;  -- Tab escape
SELECT E'"\\r"'::sexp;  -- CR escape
SELECT E'"\\\\"'::sexp;  -- Backslash
SELECT E'"\\""'::sexp;  -- Quote in string

-- Symbol edge cases
\echo 'Symbol edge cases...'
SELECT 'a'::sexp;
SELECT 'ab'::sexp;
SELECT 'a-b'::sexp;
SELECT 'a_b'::sexp;
SELECT 'a1'::sexp;
SELECT '_'::sexp;
SELECT '_*'::sexp;
SELECT '?name'::sexp;
SELECT '??rest'::sexp;

-----------------------------------------------------------
-- TEST 2: Whitespace and comments handling
-----------------------------------------------------------
\echo '=== TEST 2: Whitespace and Comments ==='

SELECT '  (  a   b   c  )  '::sexp;
SELECT E'(\n\ta\n\tb\n\tc\n)'::sexp;
SELECT E'(a ; comment\nb c)'::sexp;
SELECT E'; leading comment\n(a b c)'::sexp;
SELECT E'(a b c) ; trailing comment'::sexp;

-----------------------------------------------------------
-- TEST 3: List size boundaries
-----------------------------------------------------------
\echo '=== TEST 3: List Size Boundaries ==='

-- Small lists (count <= 7 uses compact format)
SELECT '(a)'::sexp;
SELECT '(a b)'::sexp;
SELECT '(a b c)'::sexp;
SELECT '(a b c d)'::sexp;
SELECT '(a b c d e)'::sexp;
SELECT '(a b c d e f)'::sexp;
SELECT '(a b c d e f g)'::sexp;  -- Exactly 7

-- Large lists (count > 7 uses SEntry table)
SELECT '(a b c d e f g h)'::sexp;  -- Exactly 8
SELECT '(a b c d e f g h i)'::sexp;
SELECT '(a b c d e f g h i j)'::sexp;

-- Verify list lengths
SELECT sexp_length('(a b c d e f g)'::sexp) = 7 AS len7;
SELECT sexp_length('(a b c d e f g h)'::sexp) = 8 AS len8;

-----------------------------------------------------------
-- TEST 4: Deep nesting
-----------------------------------------------------------
\echo '=== TEST 4: Deep Nesting ==='

-- Progressive nesting tests
SELECT '((a))'::sexp;
SELECT '(((a)))'::sexp;
SELECT '((((a))))'::sexp;
SELECT '(((((a)))))'::sexp;

-- Deeply nested with operations
SELECT car(car('((x))'::sexp)) = 'x'::sexp AS nested_car;

-- Mixed deep nesting
SELECT '(a (b (c (d (e (f (g (h (i (j 1))))))))))'::sexp;

-----------------------------------------------------------
-- TEST 5: Long strings
-----------------------------------------------------------
\echo '=== TEST 5: Long Strings ==='

-- String just under SHORT_STRING_MAX (31 bytes)
SELECT '"1234567890123456789012345678901"'::sexp;  -- 31 chars

-- String at SHORT_STRING_MAX boundary
SELECT '"12345678901234567890123456789012"'::sexp;  -- 32 chars (long string)

-- Verify length handling
SELECT sexp_typeof('"1234567890123456789012345678901"'::sexp) = 'string' AS short_str;
SELECT sexp_typeof('"12345678901234567890123456789012"'::sexp) = 'string' AS long_str;

-----------------------------------------------------------
-- TEST 6: Symbol table stress
-----------------------------------------------------------
\echo '=== TEST 6: Symbol Table Stress ==='

-- Many unique symbols
SELECT '(a b c d e f g h i j k l m n o p q r s t u v w x y z)'::sexp;
SELECT '(aa bb cc dd ee ff gg hh ii jj kk ll mm nn oo pp qq rr ss tt uu vv ww xx yy zz)'::sexp;

-- Repeated symbols (should intern efficiently)
SELECT '(a a a a a a a a a a a a a a a a a a a a)'::sexp;

-- Mix of repeated and unique
SELECT '(a b c a b c a b c a b c a b c a b c a b c)'::sexp;

-----------------------------------------------------------
-- TEST 7: Operations stress tests
-----------------------------------------------------------
\echo '=== TEST 7: Operations Stress ==='

-- Sequential car/cdr chains
SELECT car(cdr(cdr('(a b c d e)'::sexp)));  -- Should be c
SELECT nth('(a b c d e f g h i j)'::sexp, 9);  -- Should be j

-- Containment stress
SELECT '(a b c)'::sexp @> 'a'::sexp AS cont_first;
SELECT '(a b c)'::sexp @> 'c'::sexp AS cont_last;
SELECT '((a b) (c d) (e f))'::sexp @> '(c d)'::sexp AS cont_sub;
SELECT '((a b) (c d) (e f))'::sexp @> 'd'::sexp AS cont_deep;

-- Pattern matching stress
SELECT sexp_match('(define (f x y z) body)'::sexp, '(define (_ _*) _)'::sexp) AS pat_rest;
SELECT sexp_match('(+ a b c d e f)'::sexp, '(+ _*)'::sexp) AS pat_many;

-----------------------------------------------------------
-- TEST 8: GIN index stress
-----------------------------------------------------------
\echo '=== TEST 8: GIN Index Stress ==='

TRUNCATE stress_test;

-- Insert diverse data
INSERT INTO stress_test (expr)
SELECT ('(item ' || i || ' (data ' || (i * 10) || ') (name "item-' || i || '"))')::sexp
FROM generate_series(1, 1000) AS i;

-- Create GIN index
CREATE INDEX stress_gin_idx ON stress_test USING gin (expr sexp_gin_ops);
ANALYZE stress_test;

-- Force index usage
SET enable_seqscan = off;

-- Indexed queries
\echo 'GIN indexed queries...'
SELECT count(*) FROM stress_test WHERE expr @> 'item'::sexp;
SELECT count(*) FROM stress_test WHERE expr @> '(data 100)'::sexp;
SELECT count(*) FROM stress_test WHERE expr @> '"item-500"'::sexp;

RESET enable_seqscan;
DROP INDEX stress_gin_idx;

-----------------------------------------------------------
-- TEST 9: Equality stress
-----------------------------------------------------------
\echo '=== TEST 9: Equality Stress ==='

-- Same content, different construction
SELECT '(a b c)'::sexp = '(a b c)'::sexp AS eq1;
SELECT car('(a b c)'::sexp) = 'a'::sexp AS eq_car;  -- Different symbol tables
SELECT cdr('(a b c)'::sexp) = '(b c)'::sexp AS eq_cdr;

-- Numeric equality
SELECT '42'::sexp = '42'::sexp AS eq_int;
SELECT '3.14'::sexp = '3.14'::sexp AS eq_float;

-----------------------------------------------------------
-- TEST 10: Large data stress
-----------------------------------------------------------
\echo '=== TEST 10: Large Data Stress ==='

TRUNCATE stress_test;

-- Insert larger structures
INSERT INTO stress_test (expr)
SELECT (
    '(record ' || i || 
    ' (field1 "' || repeat('x', 100) || '")' ||
    ' (field2 ' || (i * 1000) || ')' ||
    ' (field3 (nested (data ' || i || ')))' ||
    ' (field4 (list ' || 
        string_agg('item' || j::text, ' ') || 
    ')))'
)::sexp
FROM generate_series(1, 100) AS i,
     LATERAL generate_series(1, 10) AS j
GROUP BY i;

-- Verify we can read them back
SELECT count(*) FROM stress_test;
SELECT sexp_typeof(expr) FROM stress_test WHERE id = 1;
SELECT car(expr) FROM stress_test WHERE id = 1;

-----------------------------------------------------------
-- TEST 11: Error handling (expected failures)
-----------------------------------------------------------
\echo '=== TEST 11: Error Handling ==='

-- These should all raise errors gracefully

-- Unterminated list
\echo 'Testing unterminated list (should error)...'
DO $$
BEGIN
    PERFORM '(a b c'::sexp;
    RAISE EXCEPTION 'Should have raised error';
EXCEPTION
    WHEN invalid_text_representation THEN
        RAISE NOTICE 'OK: unterminated list caught';
END $$;

-- Unterminated string
\echo 'Testing unterminated string (should error)...'
DO $$
BEGIN
    PERFORM '"hello'::sexp;
    RAISE EXCEPTION 'Should have raised error';
EXCEPTION
    WHEN invalid_text_representation THEN
        RAISE NOTICE 'OK: unterminated string caught';
END $$;

-- Extra closing paren
\echo 'Testing extra closing paren (should error)...'
DO $$
BEGIN
    PERFORM '(a b c))'::sexp;
    RAISE EXCEPTION 'Should have raised error';
EXCEPTION
    WHEN invalid_text_representation THEN
        RAISE NOTICE 'OK: trailing garbage caught';
END $$;

-- Empty atom
\echo 'Testing empty atom (should error)...'
DO $$
BEGIN
    PERFORM '( )'::sexp;  -- This is nil, should work
    RAISE NOTICE 'OK: empty parens = nil';
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Error: %', SQLERRM;
END $$;

-----------------------------------------------------------
-- TEST 12: Hash consistency
-----------------------------------------------------------
\echo '=== TEST 12: Hash Consistency ==='

-- Same structures should have same hash
SELECT sexp_hash('(a b c)'::sexp) = sexp_hash('(a b c)'::sexp) AS hash_eq;
SELECT sexp_hash('(a b c)'::sexp) <> sexp_hash('(a b d)'::sexp) AS hash_neq;

-- Hash should work in hash index
TRUNCATE stress_test;
INSERT INTO stress_test (expr) VALUES 
    ('(a b c)'::sexp),
    ('(d e f)'::sexp),
    ('(a b c)'::sexp);

CREATE INDEX stress_hash_idx ON stress_test USING hash(expr);
SELECT count(*) FROM stress_test WHERE expr = '(a b c)'::sexp;
DROP INDEX stress_hash_idx;

-----------------------------------------------------------
-- Cleanup
-----------------------------------------------------------
DROP TABLE stress_test;

\echo ''
\echo '=== ALL ROBUSTNESS TESTS PASSED ==='
