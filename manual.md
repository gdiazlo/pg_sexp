# pg_sexp Manual

## Data Types

### Atoms

```sql
-- Symbol: identifier without quotes
SELECT 'hello'::sexp;           -- hello
SELECT 'my-symbol'::sexp;       -- my-symbol

-- Integer: whole numbers
SELECT '42'::sexp;              -- 42
SELECT '-100'::sexp;            -- -100

-- Float: decimal numbers
SELECT '3.14'::sexp;            -- 3.14
SELECT '1e10'::sexp;            -- 1e10

-- String: quoted text
SELECT '"hello world"'::sexp;   -- "hello world"
SELECT '"with \"escapes\""'::sexp;

-- Nil: empty list
SELECT '()'::sexp;              -- ()
SELECT 'nil'::sexp;             -- ()
```

### Lists

```sql
-- Simple list
SELECT '(a b c)'::sexp;

-- Nested lists
SELECT '(define (square x) (* x x))'::sexp;

-- Mixed types
SELECT '(user "alice" 30 (admin true))'::sexp;
```

## Storage

pg_sexp uses a compact binary format with:
- Varint encoding for integers and lengths
- Symbol table deduplication (each unique symbol stored once)
- Small integers (-16 to 15) in single byte
- Short strings (up to 31 bytes) with inline length

Typical space usage compared to text:
- Simple atoms: 2-10 bytes
- Small lists (1-4 elements): inline format, no offset table
- Large lists (5+ elements): offset table for O(1) element access

## List Operations

### car - First Element

Returns the first element of a list.

```sql
SELECT car('(a b c)'::sexp);     -- a
SELECT car('((x y) z)'::sexp);   -- (x y)
```

**Performance**: O(1). Direct access to first element.

### cdr - Rest of List

Returns all elements except the first.

```sql
SELECT cdr('(a b c)'::sexp);     -- (b c)
SELECT cdr('(a)'::sexp);         -- ()
```

**Performance**: O(n) where n is element count. Must copy remaining elements.

### nth - Element by Index

Returns element at position (0-indexed).

```sql
SELECT nth('(a b c d e)'::sexp, 0);   -- a
SELECT nth('(a b c d e)'::sexp, 2);   -- c
SELECT nth('(a b c d e)'::sexp, 4);   -- e
```

**Performance**:
- Large lists (5+ elements): O(1). Uses offset table.
- Small lists (1-4 elements): O(n). Sequential scan, but bounded.

### sexp_length - Element Count

Returns number of elements in a list.

```sql
SELECT sexp_length('(a b c)'::sexp);       -- 3
SELECT sexp_length('()'::sexp);            -- 0
SELECT sexp_length('atom'::sexp);          -- error
```

**Performance**: O(1). Count stored in header.

### head - Alias for car

```sql
SELECT head('(a b c)'::sexp);    -- a
```

## Type Inspection

### sexp_typeof

Returns type name as text.

```sql
SELECT sexp_typeof('hello'::sexp);     -- symbol
SELECT sexp_typeof('42'::sexp);        -- integer
SELECT sexp_typeof('3.14'::sexp);      -- float
SELECT sexp_typeof('"text"'::sexp);    -- string
SELECT sexp_typeof('()'::sexp);        -- nil
SELECT sexp_typeof('(a b)'::sexp);     -- list
```

**Performance**: O(1). Type encoded in first byte.

### Type Predicates

```sql
SELECT is_symbol('hello'::sexp);    -- t
SELECT is_number('42'::sexp);       -- t (integers and floats)
SELECT is_string('"x"'::sexp);      -- t
SELECT is_nil('()'::sexp);          -- t
SELECT is_list('(a b)'::sexp);      -- t
SELECT is_atom('hello'::sexp);      -- t (non-list)
```

**Performance**: O(1). Single byte check.

## Equality

```sql
SELECT '(a b c)'::sexp = '(a b c)'::sexp;   -- t
SELECT '(a b c)'::sexp = '(a b d)'::sexp;   -- f
SELECT '(a b c)'::sexp <> '(a b d)'::sexp;  -- t
```

**Performance**: O(n) worst case. Fast path: binary comparison if symbol tables match.

## Containment

### Structural Containment (@>)

Returns true if the right operand appears as a subtree anywhere in the left operand.

```sql
-- Atom containment
SELECT '(a b c)'::sexp @> 'b';                    -- t
SELECT '(a (b c) d)'::sexp @> 'c';                -- t

-- List containment (exact subtree)
SELECT '(a (b c) d)'::sexp @> '(b c)';            -- t
SELECT '((x y) (a b))'::sexp @> '(a b)';          -- t

-- No match
SELECT '(a b c)'::sexp @> 'x';                    -- f
SELECT '(a b c)'::sexp @> '(a b)';                -- f (not exact subtree)
```

**Performance**:
- Without index: O(n*m) where n = container size, m = needle size
- With GIN index: O(log n) for index lookup + recheck

### Key-Based Containment (@>>)

Treats list heads as keys. Matches if all key-value pairs in the needle exist in the container, regardless of order.

```sql
-- Order independent
SELECT '(user (name "alice") (age 30))'::sexp 
    @>> '(name "alice")';                         -- t

SELECT '(user (age 30) (name "alice"))'::sexp 
    @>> '(name "alice")';                         -- t (order doesn't matter)

-- Multiple keys
SELECT '(config (host "localhost") (port 5432))'::sexp 
    @>> '(host "localhost")';                     -- t

-- Nested structures
SELECT '(item (meta (created "2024-01-01")))'::sexp 
    @>> '(meta (created "2024-01-01"))';          -- t
```

**Performance**: Same as structural containment.

### Contained By (<@, <<@)

Reverse of containment operators.

```sql
SELECT 'b'::sexp <@ '(a b c)'::sexp;              -- t
SELECT '(name "alice")'::sexp <<@ '(user (name "alice") (age 30))'::sexp;  -- t
```

## Pattern Matching

### Wildcards

```sql
-- _ matches any single element
SELECT '(define x 10)'::sexp @~ '(define _ _)';        -- t
SELECT '(+ 1 2)'::sexp @~ '(+ _ _)';                   -- t

-- Nested wildcards
SELECT '(define (f x) (* x x))'::sexp @~ '(define _ _)';  -- t
SELECT '(define (f x) (* x x))'::sexp @~ '(define (_ _) _)';  -- t
```

### Rest Pattern

```sql
-- _* matches zero or more elements
SELECT '(+ 1 2 3 4 5)'::sexp @~ '(+ _*)';         -- t
SELECT '(list)'::sexp @~ '(list _*)';             -- t (matches zero)
SELECT '(list a)'::sexp @~ '(list _*)';           -- t
SELECT '(list a b c)'::sexp @~ '(list _*)';       -- t
```

### sexp_find

Find first subtree matching pattern.

```sql
SELECT sexp_find('(+ (* 2 3) (* 4 5))'::sexp, '(* _ _)');
-- (* 2 3)

SELECT sexp_find('(define (square x) (* x x))'::sexp, '(* _ _)');
-- (* x x)

SELECT sexp_find('(a b c)'::sexp, '(x _)');
-- NULL (no match)
```

**Performance**: O(n). Traverses tree until match found.

## Indexing

### Hash Index

For equality queries.

```sql
CREATE INDEX idx ON my_table USING hash (expr);

-- Uses index
SELECT * FROM my_table WHERE expr = '(define x 10)'::sexp;
```

**Performance**: O(1) average for exact match.

### GIN Index

For containment queries.

```sql
CREATE INDEX idx ON my_table USING gin (expr sexp_gin_ops);

-- Uses index for @> and @>>
SELECT * FROM my_table WHERE expr @> 'define';
SELECT * FROM my_table WHERE expr @>> '(name "alice")';
```

**Key extraction strategy**:
- Atom keys: hash of each symbol, string, integer, float
- List head keys: hash of first element for lists with 3+ elements
- Pair keys: hash of (symbol, value) for 2-element lists

**Performance characteristics**:
- Index lookup: O(log n) per key
- False positives: GIN acts as bloom filter, recheck required
- Index size: ~10-50 bytes per unique element

**When to use GIN**:
- Tables with 1000+ rows
- Queries that filter by containment
- Data with repeated structural patterns

**When NOT to use GIN**:
- Small tables (sequential scan faster)
- Equality-only queries (use hash index)
- Highly unique data (poor selectivity)

## Query Examples

### Schema Design

```sql
CREATE TABLE events (
    id serial PRIMARY KEY,
    timestamp timestamptz DEFAULT now(),
    data sexp
);

CREATE INDEX events_gin ON events USING gin (data sexp_gin_ops);
```

### Inserting Data

```sql
INSERT INTO events (data) VALUES
('(click (user 123) (page "/home") (button "submit"))'::sexp),
('(view (user 123) (page "/products"))'::sexp),
('(purchase (user 456) (product "widget") (price 29.99))'::sexp);
```

### Query by Event Type

```sql
-- Find all click events
SELECT * FROM events WHERE data @> 'click';

-- Find all events for user 123
SELECT * FROM events WHERE data @>> '(user 123)';
```

**Performance with GIN index on 100k rows**: ~1-5ms

### Query by Nested Value

```sql
-- Find purchases over $20
SELECT * FROM events 
WHERE data @> 'purchase' 
  AND car(sexp_find(data, '(price _)')) = 'price';

-- Better: use application logic for numeric comparisons
```

### Extract Fields

```sql
SELECT 
    id,
    car(data) as event_type,
    sexp_find(data, '(user _)') as user_info,
    sexp_find(data, '(page _)') as page_info
FROM events;
```

### Aggregation

```sql
-- Count events by type
SELECT car(data) as event_type, count(*)
FROM events
GROUP BY car(data);
```

**Performance**: O(n) scan, car is O(1) per row.

## Performance Summary

| Operation | Complexity | Notes |
|-----------|------------|-------|
| car | O(1) | Direct access |
| cdr | O(n) | Copies remaining elements |
| nth (large list) | O(1) | Offset table lookup |
| nth (small list) | O(n) | Sequential scan, max 4 elements |
| sexp_length | O(1) | Stored in header |
| sexp_typeof | O(1) | First byte |
| is_* predicates | O(1) | First byte |
| equality | O(n) | Recursive comparison |
| @> without index | O(n*m) | n=container, m=needle |
| @> with GIN | O(log n) + recheck | Index lookup |
| @~ pattern match | O(n) | Tree traversal |
| sexp_find | O(n) | Stops at first match |
| GIN index build | O(n*k) | n=rows, k=keys per row |

## Memory Usage

- Parsing: temporary buffer proportional to input size
- Symbol table: deduplicated, shared across elements
- Operations: most are zero-copy or minimal allocation
- GIN keys: up to 1024 keys per value (capped)

## Limits

- Maximum nesting depth: 1000 levels
- Maximum symbol table: 65536 unique symbols per value
- Maximum GIN keys per value: 1024
- Maximum list offset: 256MB (28-bit offset in large lists)
