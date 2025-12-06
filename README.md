# pg_sexp

S-expression data type for PostgreSQL. Similar to jsonb but for Lisp-like s-expressions.

## Features

- Native sexp data type with compact binary storage
- List operations: `car`, `cdr`, `nth`, `head`, `sexp_length`
- Type inspection: `sexp_typeof`, `is_list`, `is_atom`, `is_symbol`, `is_string`, `is_number`, `is_nil`
- Structural containment operator `@>` with GIN index support
- Key-based containment operator `@>>` (treats list heads as keys, order-independent)
- Pattern matching with wildcards (`_`), rest patterns (`_*`), and captures (`?name`, `??rest`)
- Hash index support for equality queries

## Syntax

```sql
-- Atoms
SELECT 'hello'::sexp;           -- symbol
SELECT '42'::sexp;              -- integer
SELECT '3.14'::sexp;            -- float
SELECT '"hello world"'::sexp;   -- string
SELECT '()'::sexp;              -- nil

-- Lists
SELECT '(a b c)'::sexp;
SELECT '(define x 10)'::sexp;
SELECT '(lambda (x) (* x x))'::sexp;
```

## Operations

```sql
-- List operations
SELECT car('(a b c)'::sexp);           -- a
SELECT cdr('(a b c)'::sexp);           -- (b c)
SELECT nth('(a b c d)'::sexp, 2);      -- c
SELECT sexp_length('(a b c)'::sexp);   -- 3

-- Type inspection
SELECT sexp_typeof('hello'::sexp);     -- symbol
SELECT is_list('(a b)'::sexp);         -- t

-- Containment (structural)
SELECT '(a (b c) d)'::sexp @> '(b c)'; -- t

-- Key-based containment
SELECT '(user (name "alice") (age 30))'::sexp @>> '(name "alice")'; -- t

-- Pattern matching
SELECT '(define x 10)'::sexp @~ '(define _ _)';  -- t
SELECT sexp_find('(a (b (c d)) e)'::sexp, '(c _)'); -- (c d)
```

## GIN Index

```sql
CREATE INDEX idx ON my_table USING gin (expr sexp_gin_ops);

-- Indexed queries
SELECT * FROM my_table WHERE expr @> 'define';
SELECT * FROM my_table WHERE expr @>> '(user (name "alice"))';
```

## Building

### Requirements

- PostgreSQL 14+ with development headers
- C compiler (gcc or clang)
- make

### Native Build

Ensure `pg_config` is in your PATH or specify it:

```sh
make PG_CONFIG=/path/to/pg_config
make install
```

### Building for a Specific PostgreSQL Version

Download PostgreSQL source and build:

```sh
wget https://ftp.postgresql.org/pub/source/v18.1/postgresql-18.1.tar.bz2
tar xjf postgresql-18.1.tar.bz2
cd postgresql-18.1
./configure --prefix=/opt/pgsql-18.1
make -j$(nproc)
make install
```

Build pg_sexp against it:

```sh
make PG_CONFIG=/opt/pgsql-18.1/bin/pg_config clean all
make PG_CONFIG=/opt/pgsql-18.1/bin/pg_config install
```

### Container Build (Recommended)

Build artifacts for the host system using Docker/Podman:

```sh
# Build using PostgreSQL 16
podman build -f Containerfile.build -t pg_sexp-build .
podman run --rm -v $(pwd):/src:Z pg_sexp-build
```

To target a different PostgreSQL version, edit `Containerfile.build` and change the base image and dev package.

## Testing

Run the test suite in a container:

```sh
make container-test
# or
podman build -f Containerfile.test -t pg_sexp-test .
podman run --rm pg_sexp-test
```

### Memory Safety Tests

```sh
# AddressSanitizer (fast, ~2x overhead)
make asan-test

# Valgrind (thorough, ~10-20x overhead)
make valgrind-test

# Both
make memory-test
```

## Installation

### From Built Artifacts

Copy the shared library and SQL files to your PostgreSQL installation:

```sh
cp pg_sexp.so $(pg_config --pkglibdir)/
cp pg_sexp.control $(pg_config --sharedir)/extension/
cp sql/pg_sexp--0.1.0.sql $(pg_config --sharedir)/extension/
```

### Enable in Database

```sql
CREATE EXTENSION pg_sexp;
```

## Container Deployment

### Test Container

For development and testing:

```sh
podman build -f Containerfile.test -t pg_sexp-test .
podman run --rm -it pg_sexp-test bash
```

### Production Container

Includes PostgreSQL 18.1, TimescaleDB, and pg_sexp:

```sh
podman build -f Containerfile.production -t pg_sexp-production .

# Run as daemon
podman run -d --name pg_sexp-prod \
    -v pg_sexp_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    pg_sexp-production

# Connect
psql -h localhost -U postgres -d tsdb
```

The production container:
- Initializes with TimescaleDB and pg_sexp extensions
- Creates a `tsdb` database with extensions pre-loaded
- Configures production-ready PostgreSQL settings
- Supports distributed TimescaleDB clusters

### TimescaleDB Testing

Test compatibility with TimescaleDB hypertables and compression:

```sh
make timescaledb-test
```

## Container Build Targets

| Target | Description |
|--------|-------------|
| `container-test` | Run tests in isolated container |
| `asan-test` | AddressSanitizer memory safety |
| `asan-stress` | ASan with stress workload |
| `valgrind-test` | Valgrind memcheck |
| `valgrind-full` | Valgrind full test suite |
| `memory-test` | Run all memory tests |
| `timescaledb-test` | TimescaleDB compatibility |
| `production-build` | Build production image |
| `production-test` | Test production image |
| `production-run` | Run production container |

## Project Structure

```
pg_sexp/
  src/
    pg_sexp.c       -- Extension entry point
    pg_sexp.h       -- Data type definitions
    sexp_parser.c   -- Text to binary parser
    sexp_io.c       -- Input/output functions
    sexp_ops.c      -- Operations and containment
    sexp_match.c    -- Pattern matching
    sexp_gin.c      -- GIN index support
  sql/
    pg_sexp--0.1.0.sql  -- Extension SQL definitions
  test/
    test_basic.sql      -- Basic functionality tests
    test_robustness.sql -- Edge cases and stress tests
  Containerfile.*       -- Container definitions
  Makefile
  pg_sexp.control
```

## License

See LICENSE file.
