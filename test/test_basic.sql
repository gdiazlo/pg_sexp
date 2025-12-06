-- Basic tests for pg_sexp extension

\echo '=== Testing pg_sexp extension ==='

-- Test parsing atoms
\echo 'Testing atoms...'
SELECT 'hello'::sexp;
SELECT '42'::sexp;
SELECT '3.14'::sexp;
SELECT '"hello world"'::sexp;
SELECT 'nil'::sexp;
SELECT '()'::sexp;

-- Test parsing lists
\echo 'Testing lists...'
SELECT '(a b c)'::sexp;
SELECT '(+ 1 2)'::sexp;
SELECT '(define x 10)'::sexp;
SELECT '((nested) (lists) (here))'::sexp;

-- Test type inspection
\echo 'Testing type inspection...'
SELECT sexp_typeof('hello'::sexp);
SELECT sexp_typeof('42'::sexp);
SELECT sexp_typeof('3.14'::sexp);
SELECT sexp_typeof('"string"'::sexp);
SELECT sexp_typeof('()'::sexp);
SELECT sexp_typeof('(a b c)'::sexp);

-- Test type predicates
\echo 'Testing type predicates...'
SELECT is_symbol('hello'::sexp);
SELECT is_number('42'::sexp);
SELECT is_string('"test"'::sexp);
SELECT is_nil('()'::sexp);
SELECT is_list('(a b c)'::sexp);
SELECT is_atom('hello'::sexp);

-- Test list operations
\echo 'Testing list operations...'
SELECT car('(a b c)'::sexp);
SELECT cdr('(a b c)'::sexp);
SELECT sexp_length('(a b c d e)'::sexp);
SELECT nth('(a b c d e)'::sexp, 0);
SELECT nth('(a b c d e)'::sexp, 2);
SELECT nth('(a b c d e)'::sexp, 4);

-- Test nested structures
\echo 'Testing nested structures...'
SELECT '(define (square x) (* x x))'::sexp;
SELECT car('(define (square x) (* x x))'::sexp);
SELECT nth('(define (square x) (* x x))'::sexp, 1);
SELECT car(nth('(define (square x) (* x x))'::sexp, 2));

-- Test equality
\echo 'Testing equality...'
SELECT '(a b c)'::sexp = '(a b c)'::sexp;
SELECT '(a b c)'::sexp = '(a b d)'::sexp;
SELECT '(a b c)'::sexp <> '(a b d)'::sexp;

-- Test containment
\echo 'Testing containment...'
SELECT '(a b c)'::sexp @> 'b'::sexp;
SELECT '((a b) (c d))'::sexp @> '(c d)'::sexp;
SELECT '(a b c)'::sexp @> 'x'::sexp;

-- Test nested containment (benchmark scenario)
\echo 'Testing nested containment...'
SELECT '(item 1 (name "test") (tags (a b c)))'::sexp @> '(tags (a b c))'::sexp AS exact_sublist_match;
SELECT '(item 1 (name "test") (tags (a b c)))'::sexp @> 'tags'::sexp AS atom_in_nested;
SELECT '(item 1 (name "test") (tags (a b c)))'::sexp @> '(a b c)'::sexp AS inner_sublist_match;

-- Test with table
\echo 'Testing table storage...'
CREATE TABLE sexp_test (id serial PRIMARY KEY, expr sexp);
INSERT INTO sexp_test (expr) VALUES 
    ('(define x 10)'::sexp),
    ('(+ 1 2 3)'::sexp),
    ('(lambda (x) (* x x))'::sexp),
    ('(if (> x 0) "positive" "non-positive")'::sexp);

SELECT * FROM sexp_test;
SELECT id, car(expr), sexp_length(expr) FROM sexp_test;

-- Test hash index
\echo 'Testing hash index...'
CREATE INDEX sexp_test_hash ON sexp_test USING hash(expr);
SELECT * FROM sexp_test WHERE expr = '(+ 1 2 3)'::sexp;

-- Clean up
DROP TABLE sexp_test;

-- Test pattern matching
\echo 'Testing pattern matching...'

-- Basic wildcard matching
\echo 'Basic wildcard tests...'
SELECT sexp_match('(define x 10)'::sexp, '(define _ _)'::sexp) AS define_any_2;
SELECT sexp_match('(define x 10)'::sexp, '(define x _)'::sexp) AS define_x_any;
SELECT sexp_match('(define x 10)'::sexp, '(define _ 10)'::sexp) AS define_any_10;
SELECT sexp_match('(define x 10)'::sexp, '(define y _)'::sexp) AS define_y_any_should_fail;
SELECT sexp_match('(+ 1 2)'::sexp, '(+ _ _)'::sexp) AS plus_any_any;

-- Nested pattern matching
\echo 'Nested pattern tests...'
SELECT sexp_match('(define (square x) (* x x))'::sexp, '(define _ _)'::sexp) AS nested_match;
SELECT sexp_match('(define (square x) (* x x))'::sexp, '(define (square _) _)'::sexp) AS nested_match_2;

-- Rest/spread pattern (_*)
\echo 'Rest pattern tests...'
SELECT sexp_match('(+ 1 2 3 4 5)'::sexp, '(+ _*)'::sexp) AS plus_rest;
SELECT sexp_match('(+ 1)'::sexp, '(+ _*)'::sexp) AS plus_one_rest;
SELECT sexp_match('(+)'::sexp, '(+ _*)'::sexp) AS plus_empty_rest;
SELECT sexp_match('(list a b c d e)'::sexp, '(list _*)'::sexp) AS list_rest;

-- Operator syntax
\echo 'Operator syntax tests...'
SELECT '(define x 10)'::sexp @~ '(define _ _)'::sexp AS op_match;
SELECT '(define x 10)'::sexp @~ '(set! _ _)'::sexp AS op_no_match;

-- sexp_find tests
\echo 'Find pattern tests...'
SELECT sexp_find('(+ (* 2 3) (* 4 5))'::sexp, '(* _ _)'::sexp) AS find_mult;
SELECT sexp_find('(define (square x) (* x x))'::sexp, '(* _ _)'::sexp) AS find_mult_in_define;
SELECT sexp_find('(a b c)'::sexp, '(x y)'::sexp) AS find_no_match;

-- Practical example: find all function definitions
\echo 'Practical pattern matching...'
CREATE TABLE code_sexp (id serial PRIMARY KEY, code sexp);
INSERT INTO code_sexp (code) VALUES
    ('(define (add a b) (+ a b))'::sexp),
    ('(define (square x) (* x x))'::sexp),
    ('(define pi 3.14159)'::sexp),
    ('(lambda (x) (* x 2))'::sexp),
    ('(if (> x 0) x (- x))'::sexp);

-- Find all 'define' forms
SELECT id, code FROM code_sexp WHERE code @~ '(define _*)'::sexp;

-- Find function definitions (define with a list as second element)
SELECT id, code FROM code_sexp WHERE code @~ '(define (_ _*) _)'::sexp;

-- Find value definitions (define with symbol as second element)
-- This would need a "not list" pattern which we don't have yet

DROP TABLE code_sexp;

-- Test GIN index support
\echo '=== Testing GIN Index Support ==='

-- Create test table
CREATE TABLE gin_test (id serial PRIMARY KEY, expr sexp);

-- Insert diverse test data
INSERT INTO gin_test (expr) VALUES
    ('(define x 10)'::sexp),
    ('(define y 20)'::sexp),
    ('(define (square n) (* n n))'::sexp),
    ('(define (add a b) (+ a b))'::sexp),
    ('(+ 1 2 3)'::sexp),
    ('(* 4 5)'::sexp),
    ('(lambda (x) (* x x))'::sexp),
    ('(if (> x 0) "positive" "negative")'::sexp),
    ('(let ((x 1) (y 2)) (+ x y))'::sexp),
    ('(item 1 (name "test") (tags (a b c)))'::sexp);

\echo 'Testing sequential scan containment (before index)...'
-- Test containment of a symbol (should find rows containing the symbol 'define')
\echo 'Seq scan: rows containing symbol define:'
SELECT id, expr FROM gin_test WHERE expr @> 'define'::sexp;

-- Test containment of a sublist
\echo 'Seq scan: rows containing exact sublist (tags (a b c)):'
SELECT id, expr FROM gin_test WHERE expr @> '(tags (a b c))'::sexp;

-- Create GIN index
\echo 'Creating GIN index...'
CREATE INDEX gin_test_idx ON gin_test USING gin (expr sexp_gin_ops);

-- Force index usage
SET enable_seqscan = off;

\echo 'Testing indexed containment queries...'
-- Same queries with index
\echo 'GIN index: rows containing symbol define:'
EXPLAIN (COSTS OFF) SELECT * FROM gin_test WHERE expr @> 'define'::sexp;
SELECT id, expr FROM gin_test WHERE expr @> 'define'::sexp;

\echo 'GIN index: rows containing exact sublist (tags (a b c)):'
EXPLAIN (COSTS OFF) SELECT * FROM gin_test WHERE expr @> '(tags (a b c))'::sexp;
SELECT id, expr FROM gin_test WHERE expr @> '(tags (a b c))'::sexp;

\echo 'Testing indexed containment with exact list match...'
SELECT id, expr FROM gin_test WHERE expr @> '(+ 1 2 3)'::sexp;

\echo 'Testing indexed containment with nested sublist...'
SELECT id, expr FROM gin_test WHERE expr @> '(* n n)'::sexp;

\echo 'Testing no matches...'
SELECT id, expr FROM gin_test WHERE expr @> '(nonexistent)'::sexp;

-- Reset
SET enable_seqscan = on;

-- Verify index is actually used with larger dataset
\echo 'Testing with larger dataset...'
INSERT INTO gin_test (expr)
SELECT ('(item ' || i || ' (data ' || (i * 10) || '))')::sexp
FROM generate_series(1, 1000) AS i;

-- Re-analyze for accurate statistics
ANALYZE gin_test;

-- This should use the index for selective queries
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM gin_test WHERE expr @> '(define (square n) (* n n))'::sexp;
SELECT id, expr FROM gin_test WHERE expr @> '(define (square n) (* n n))'::sexp;
SET enable_seqscan = on;

DROP TABLE gin_test;

\echo '=== All tests passed! ==='
