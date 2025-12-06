/*
 * pg_sexp.c
 *
 * Main PostgreSQL extension entry point and SQL-callable functions
 */

#include "pg_sexp.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"
#include "common/hashfn.h"

PG_MODULE_MAGIC;

/* Input/Output functions */
PG_FUNCTION_INFO_V1(sexp_in);
PG_FUNCTION_INFO_V1(sexp_out);
PG_FUNCTION_INFO_V1(sexp_send);
PG_FUNCTION_INFO_V1(sexp_recv);

/* Operators */
PG_FUNCTION_INFO_V1(sexp_eq);
PG_FUNCTION_INFO_V1(sexp_ne);

/* List operations */
PG_FUNCTION_INFO_V1(sexp_car_func);
PG_FUNCTION_INFO_V1(sexp_cdr_func);
PG_FUNCTION_INFO_V1(sexp_length_func);
PG_FUNCTION_INFO_V1(sexp_nth_func);
PG_FUNCTION_INFO_V1(sexp_head_func);

/* Type inspection */
PG_FUNCTION_INFO_V1(sexp_typeof);
PG_FUNCTION_INFO_V1(sexp_is_nil);
PG_FUNCTION_INFO_V1(sexp_is_list);
PG_FUNCTION_INFO_V1(sexp_is_atom);
PG_FUNCTION_INFO_V1(sexp_is_symbol);
PG_FUNCTION_INFO_V1(sexp_is_string);
PG_FUNCTION_INFO_V1(sexp_is_number);

/* Containment */
PG_FUNCTION_INFO_V1(sexp_contains_func);
PG_FUNCTION_INFO_V1(sexp_contains_key_func);

/* Pattern matching */
PG_FUNCTION_INFO_V1(sexp_match_func);
PG_FUNCTION_INFO_V1(sexp_find_func);

/* Hash support */
PG_FUNCTION_INFO_V1(sexp_hash);

/*
 * sexp_in - Parse text representation into sexp
 */
Datum
sexp_in(PG_FUNCTION_ARGS)
{
    char   *input = PG_GETARG_CSTRING(0);
    Sexp   *result;

    result = sexp_parse(input, strlen(input));
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_out - Convert sexp to text representation
 */
Datum
sexp_out(PG_FUNCTION_ARGS)
{
    Sexp   *sexp = PG_GETARG_SEXP(0);
    char   *result;

    result = sexp_to_cstring(sexp);
    
    PG_RETURN_CSTRING(result);
}

/*
 * sexp_recv - Binary input
 */
Datum
sexp_recv(PG_FUNCTION_ARGS)
{
    StringInfo  buf = (StringInfo) PG_GETARG_POINTER(0);
    Sexp       *result;
    int         nbytes;
    
    nbytes = buf->len - buf->cursor;
    result = (Sexp *) palloc(nbytes + VARHDRSZ);
    SET_VARSIZE(result, nbytes + VARHDRSZ);
    memcpy(VARDATA(result), &buf->data[buf->cursor], nbytes);
    buf->cursor += nbytes;
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_send - Binary output
 */
Datum
sexp_send(PG_FUNCTION_ARGS)
{
    Sexp       *sexp = PG_GETARG_SEXP(0);
    StringInfoData buf;
    
    pq_begintypsend(&buf);
    pq_sendbytes(&buf, (char *) VARDATA(sexp), VARSIZE(sexp) - VARHDRSZ);
    
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * sexp_eq - Equality operator
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 */
Datum
sexp_eq(PG_FUNCTION_ARGS)
{
    struct varlena *a = PG_GETARG_SEXP_PACKED(0);
    struct varlena *b = PG_GETARG_SEXP_PACKED(1);
    
    PG_RETURN_BOOL(sexp_equal_packed(a, b));
}

/*
 * sexp_ne - Inequality operator
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 */
Datum
sexp_ne(PG_FUNCTION_ARGS)
{
    struct varlena *a = PG_GETARG_SEXP_PACKED(0);
    struct varlena *b = PG_GETARG_SEXP_PACKED(1);
    
    PG_RETURN_BOOL(!sexp_equal_packed(a, b));
}

/*
 * sexp_car_func - Get first element of a list (head)
 */
Datum
sexp_car_func(PG_FUNCTION_ARGS)
{
    Sexp   *sexp = PG_GETARG_SEXP(0);
    Sexp   *result;
    
    result = sexp_car(sexp);
    if (result == NULL)
        PG_RETURN_NULL();
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_cdr_func - Get rest of list (tail)
 */
Datum
sexp_cdr_func(PG_FUNCTION_ARGS)
{
    Sexp   *sexp = PG_GETARG_SEXP(0);
    Sexp   *result;
    
    result = sexp_cdr(sexp);
    if (result == NULL)
        PG_RETURN_NULL();
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_length_func - Get number of elements in a list
 */
Datum
sexp_length_func(PG_FUNCTION_ARGS)
{
    Sexp   *sexp = PG_GETARG_SEXP(0);
    
    PG_RETURN_INT32(sexp_length(sexp));
}

/*
 * sexp_nth_func - Get nth element of a list (0-indexed)
 */
Datum
sexp_nth_func(PG_FUNCTION_ARGS)
{
    Sexp   *sexp = PG_GETARG_SEXP(0);
    int32   n = PG_GETARG_INT32(1);
    Sexp   *result;
    
    result = sexp_nth(sexp, n);
    if (result == NULL)
        PG_RETURN_NULL();
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_head_func - Get first element (same as car but named differently)
 */
Datum
sexp_head_func(PG_FUNCTION_ARGS)
{
    Sexp   *sexp = PG_GETARG_SEXP(0);
    Sexp   *result;
    
    result = sexp_head(sexp);
    if (result == NULL)
        PG_RETURN_NULL();
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_typeof - Get type name of sexp
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 */
Datum
sexp_typeof(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    const char *typename;
    
    switch (SEXP_TYPE_PACKED(packed))
    {
        case SEXP_NIL:
            typename = "nil";
            break;
        case SEXP_SYMBOL:
            typename = "symbol";
            break;
        case SEXP_STRING:
            typename = "string";
            break;
        case SEXP_INTEGER:
            typename = "integer";
            break;
        case SEXP_FLOAT:
            typename = "float";
            break;
        case SEXP_LIST:
            typename = "list";
            break;
        default:
            typename = "unknown";
            break;
    }
    
    PG_RETURN_TEXT_P(cstring_to_text(typename));
}

/*
 * Type predicate functions
 * 
 * OPTIMIZED: Use packed varlena for all type checks to avoid detoasting copy.
 */
Datum
sexp_is_nil(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    PG_RETURN_BOOL(SEXP_TYPE_PACKED(packed) == SEXP_NIL);
}

Datum
sexp_is_list(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    SexpType t = SEXP_TYPE_PACKED(packed);
    PG_RETURN_BOOL(t == SEXP_LIST || t == SEXP_NIL);
}

Datum
sexp_is_atom(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    SexpType t = SEXP_TYPE_PACKED(packed);
    PG_RETURN_BOOL(t == SEXP_SYMBOL || t == SEXP_STRING || 
                   t == SEXP_INTEGER || t == SEXP_FLOAT);
}

Datum
sexp_is_symbol(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    PG_RETURN_BOOL(SEXP_TYPE_PACKED(packed) == SEXP_SYMBOL);
}

Datum
sexp_is_string(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    PG_RETURN_BOOL(SEXP_TYPE_PACKED(packed) == SEXP_STRING);
}

Datum
sexp_is_number(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    SexpType t = SEXP_TYPE_PACKED(packed);
    PG_RETURN_BOOL(t == SEXP_INTEGER || t == SEXP_FLOAT);
}

/*
 * sexp_contains_func - Check if container contains element (structural)
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 */
Datum
sexp_contains_func(PG_FUNCTION_ARGS)
{
    struct varlena *container = PG_GETARG_SEXP_PACKED(0);
    struct varlena *element = PG_GETARG_SEXP_PACKED(1);
    
    PG_RETURN_BOOL(sexp_contains_packed(container, element));
}

/*
 * sexp_contains_key_func - Check if container contains element (key-based)
 * 
 * Key-based containment (@>>) treats list heads as "keys" and matches
 * remaining elements in any order, similar to jsonb @>.
 */
Datum
sexp_contains_key_func(PG_FUNCTION_ARGS)
{
    Sexp   *container = PG_GETARG_SEXP(0);
    Sexp   *needle = PG_GETARG_SEXP(1);
    
    PG_RETURN_BOOL(sexp_contains_key(container, needle));
}

/*
 * sexp_match_func - Pattern matching (SQL wrapper)
 */
Datum
sexp_match_func(PG_FUNCTION_ARGS)
{
    Sexp   *expr = PG_GETARG_SEXP(0);
    Sexp   *pattern = PG_GETARG_SEXP(1);
    
    PG_RETURN_BOOL(sexp_match(expr, pattern));
}

/*
 * sexp_find_func - Find first matching subexpression (SQL wrapper)
 */
Datum
sexp_find_func(PG_FUNCTION_ARGS)
{
    Sexp   *expr = PG_GETARG_SEXP(0);
    Sexp   *pattern = PG_GETARG_SEXP(1);
    Sexp   *result;
    
    result = sexp_find_first(expr, pattern);
    
    if (result == NULL)
        PG_RETURN_NULL();
    
    PG_RETURN_SEXP(result);
}

/*
 * sexp_hash - Semantic hash function for hash indexes and joins
 *
 * IMPORTANT: This must compute a semantic hash, not a raw bytes hash.
 * Two semantically equal sexp values can have different binary representations
 * due to symbol table ordering differences. For example:
 *   - car('(a b c)') has symbol table ["a","b","c"], symbol 'a' is index 0
 *   - 'a' parsed fresh has symbol table ["a"], symbol 'a' is index 0
 * Both represent the symbol 'a' but have different bytes.
 *
 * The semantic hash mirrors equality semantics:
 *   - Symbols: hash the symbol STRING, not the symbol ID
 *   - Strings: hash the string content
 *   - Integers: hash the canonical 64-bit zigzag-decoded value
 *   - Floats: hash the 64-bit bit pattern (normalizing -0.0 to 0.0)
 *   - Lists: combine child hashes with position mixing
 *   - NIL: returns 0
 *
 * Uses PostgreSQL's native hash_bytes/hash_bytes_uint32 for mixing.
 *
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 */
Datum
sexp_hash(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    uint32  hash;
    
    /* Use the semantic hash computation from sexp_ops.c */
    hash = sexp_compute_hash_packed(packed);
    
    PG_RETURN_INT32((int32)hash);
}

/*
 * sexp_hash_extended - Extended hash function with seed for parallel operations
 *
 * This is used by PostgreSQL's parallel hash joins and aggregates.
 * The seed allows different workers to use different hash functions
 * to reduce adversarial collision risks.
 *
 * When seed is 0, returns the same value as sexp_hash().
 * Otherwise, mixes the base hash with the seed.
 *
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 */
PG_FUNCTION_INFO_V1(sexp_hash_extended);
Datum
sexp_hash_extended(PG_FUNCTION_ARGS)
{
    struct varlena *packed = PG_GETARG_SEXP_PACKED(0);
    int64   seed = PG_GETARG_INT64(1);
    uint64  hash;
    
    /* Start with the semantic hash */
    hash = (uint64)sexp_compute_hash_packed(packed);
    
    /* Mix with seed if provided */
    if (seed != 0)
    {
        /* 
         * Use a simple but effective mixing function.
         * XOR with rotated seed provides good distribution.
         */
        uint64 mixed_seed = (uint64)seed;
        mixed_seed = (mixed_seed << 32) | (mixed_seed >> 32);  /* swap halves */
        hash = hash ^ mixed_seed;
        hash = hash * 0x9e3779b97f4a7c15ULL;  /* golden ratio constant */
        hash = (hash >> 32) ^ hash;  /* final mix */
    }
    
    PG_RETURN_INT64((int64)hash);
}
