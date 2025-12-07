# pg_sexp Benchmark Comparison: C vs Rust vs JSONB

**Date:** $(date)
**PostgreSQL:** 18.1
**Hardware:** Container environment

## Summary

| Metric | C (pg_sexp) | Rust (pg_sexp_rs) | JSONB | Notes |
|--------|-------------|-------------------|-------|-------|
| **Bulk Insert (500K rows)** | 3,037 ms | 3,228 ms | 1,758 ms | JSONB wins |
| **Storage Size (500K)** | 206 MB | 255 MB | 241 MB | C is most compact |
| **Avg Row Size** | 409 bytes | 511 bytes | 481 bytes | C is most compact |

## Detailed Results

### TEST 1: BULK INSERT (500,000 rows)

| Implementation | Time |
|----------------|------|
| C (pg_sexp) | 3,037 ms |
| Rust (pg_sexp_rs) | 3,228 ms |
| JSONB | 1,758 ms |

**Winner: JSONB** (73% faster than C, 84% faster than Rust)

### TEST 2: STORAGE SIZE

| Implementation | Total Size | Table Size | Avg Row |
|----------------|------------|------------|---------|
| C (pg_sexp) | 206 MB | 195 MB | 409 bytes |
| Rust (pg_sexp_rs) | 255 MB | 244 MB | 511 bytes |
| JSONB | 241 MB | 230 MB | 481 bytes |

**Winner: C** (17% smaller than JSONB, 24% smaller than Rust)

### TEST 3: SEQUENTIAL SCAN

| Operation | C | Rust | JSONB |
|-----------|---|------|-------|
| Full COUNT | 13.5 ms | 18.8 ms | 14.8 ms |
| SUM length | 107 ms | 440 ms | 143 ms |

**Winner: C** (serialization ~25% faster than JSONB, ~4x faster than Rust)

### TEST 4: ELEMENT ACCESS

| Operation | C | Rust | JSONB |
|-----------|---|------|-------|
| car / ? key | 53 ms | 165 ms | 29 ms |
| nth(2) / ->'user' | 34 ms | 253 ms | 32 ms |
| Deep nested | 114 ms | 626 ms | 35 ms |

**Winner: JSONB** (path-based access is optimized)

### TEST 5: KEY-BASED CONTAINMENT (@>> / @>)

| Query | C @>> | Rust @>> | JSONB @> |
|-------|-------|----------|----------|
| theme="dark" (33%) | 147 ms | 337 ms | 38 ms |
| user.id=12345 | 151 ms | 363 ms | 34 ms |
| Multi-condition | 137 ms | 420 ms | 40 ms |

**Winner: JSONB** (~4x faster than C, ~10x faster than Rust)

### TEST 5b: STRUCTURAL CONTAINMENT (sexp @> only)

| Query | C | Rust |
|-------|---|------|
| (theme "dark") | 138 ms | 310 ms |
| symbol 'alpha' | 144 ms | 305 ms |
| string "dark" | 129 ms | 306 ms |

**Winner: C** (~2.2x faster than Rust)

### TEST 6: AST-LIKE STRUCTURES (200K rows)

| Metric | C | Rust | JSONB |
|--------|---|------|-------|
| Insert time | 1,412 ms | 1,213 ms | 1,403 ms |
| **Storage size** | **67 MB** | **82 MB** | **265 MB** |
| Find 'let' | 67 ms | 84 ms | 40 ms |
| Find 'cond' | 62 ms | 107 ms | 148 ms |

**Winner for storage: C** (75% smaller than JSONB!)
**Winner for query: Depends** (sexp better for LIKE-style searches)

### TEST 7: DEEPLY NESTED (100K rows, 15 levels)

| Metric | C | Rust | JSONB |
|--------|---|------|-------|
| Insert time | 262 ms | 429 ms | 750 ms |
| Storage | 27 MB | 39 MB | 43 MB |
| Deep containment | 20 ms | 72 ms | 29 ms |

**Winner: C** (fastest insert, smallest storage, fastest query)

### TEST 8: GIN INDEX PERFORMANCE

| Metric | C | Rust | JSONB |
|--------|---|------|-------|
| Index creation | 2,190 ms | N/A* | 1,080 ms |
| Index size | 115 MB | N/A* | 68 MB |
| GIN click events | 71 ms | N/A* | 38 ms |
| GIN highly selective | 0.17 ms | N/A* | 0.08 ms |

*Note: Rust GIN operator class not yet registered

**Winner: JSONB** (smaller index, faster queries)

### TEST 9: SERIALIZATION

| Implementation | Time |
|----------------|------|
| C | 150 ms |
| Rust | 486 ms |
| JSONB | 166 ms |

**Winner: C** (10% faster than JSONB, 3x faster than Rust)

### TEST 10: LIST OPERATIONS (sexp only)

| Operation | C | Rust |
|-----------|---|------|
| car | 50 ms | 168 ms |
| cdr | 70 ms | 568 ms |
| sexp_length | 31 ms | 120 ms |
| nth(3) | 34 ms | 238 ms |

**Winner: C** (3-8x faster for all operations)

## Key Takeaways

### C Implementation (pg_sexp)
- ✅ Most compact storage (especially for AST-like structures)
- ✅ Fastest for list operations (car, cdr, nth)
- ✅ Excellent for deeply nested data
- ✅ GIN index support working
- ⚠️ Slower than JSONB for key-based queries

### Rust Implementation (pg_sexp_rs)
- ✅ Functional and correct
- ✅ Similar semantics to C version
- ⚠️ 3-5x slower than C for most operations
- ⚠️ Larger storage footprint
- ❌ GIN index not yet registered (needs operator class)
- 📝 Room for optimization (binary format, algorithms)

### JSONB (baseline)
- ✅ Fastest for key-based queries
- ✅ Mature GIN implementation
- ⚠️ Much larger storage for AST-like structures
- ❌ No structural containment like sexp @>
- ❌ No pattern matching

## Recommendations

1. **Use C pg_sexp for:**
   - Code/AST storage (75% smaller than JSONB)
   - Deep nesting (fastest insert/query)
   - Structural pattern matching
   - List-oriented access patterns

2. **Use JSONB for:**
   - Key-value lookups
   - When GIN index performance is critical
   - Standard JSON interoperability

3. **Rust implementation needs:**
   - Binary format optimization
   - GIN operator class registration
   - Algorithm improvements for containment
