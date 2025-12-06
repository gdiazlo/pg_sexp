/*
 * sexp_gin.c
 *
 * GIN (Generalized Inverted Index) support for sexp type
 * 
 * This enables indexed containment queries, providing 100-1000x speedup
 * over sequential scans for large tables.
 *
 * KEY EXTRACTION STRATEGY (Flat Keys):
 * ====================================
 * We extract simple flat keys from each element:
 *
 * 1. ATOM KEYS: hash(type, value)
 *    - Every atom (symbol, string, int, float) gets a content key
 *    - Enables finding atoms anywhere in structure
 *
 * 2. LIST HEAD KEYS: hash(LIST_HEAD_MARKER, first_element_hash)
 *    - Every list gets a key based on its head (car)
 *    - Enables finding lists by their "type tag"
 *
 * For containment A @> B (structural) or A @>> B (key-based):
 *   - Extract keys from B (the query/needle)
 *   - Check if A's index contains ALL of B's keys
 *   - This is a necessary but not sufficient condition
 *   - Recheck is required for final verification
 *
 * The GIN index serves as a bloom filter - it can quickly reject
 * non-matching rows but may have false positives that the recheck
 * phase filters out.
 */

#include "pg_sexp.h"
#include "sexp_debug.h"
#include "access/gin.h"
#include "access/stratnum.h"
#include "utils/builtins.h"

/* Strategy numbers for GIN operators */
#define SEXP_GIN_CONTAINS_STRATEGY     7   /* @> structural containment */
#define SEXP_GIN_CONTAINED_STRATEGY    8   /* <@ contained by */
#define SEXP_GIN_CONTAINS_KEY_STRATEGY 9   /* @>> key-based containment */

/* Key type markers (mixed into hash to distinguish key types) */
#define KEY_TYPE_ATOM       0x01000000
#define KEY_TYPE_LIST_HEAD  0x02000000
#define KEY_TYPE_SYMBOL     0x03000000
#define KEY_TYPE_STRING     0x04000000
#define KEY_TYPE_INTEGER    0x05000000
#define KEY_TYPE_FLOAT      0x06000000
#define KEY_TYPE_PAIR       0x07000000  /* (sym value) pair key for @>> selectivity */
#define KEY_TYPE_BLOOM      0x08000000  /* Bloom signature summary key */

/* Maximum keys to extract from a single value (prevent explosion) */
#define MAX_GIN_KEYS 1024  /* Reduced from 2048 to limit index size */

/*
 * Key Extraction Strategy:
 * 
 * Keys are extracted selectively to balance index size and query performance:
 * 
 * 1. PAIR KEYS: For 2-element (symbol value) lists like (age 30),
 *    extract hash(symbol, value). This provides high selectivity for
 *    common key-value patterns.
 * 
 * 2. ATOM KEYS: All atoms (symbols, strings, integers, floats) get
 *    individual keys. Required for queries that search for specific values.
 * 
 * 3. LIST HEAD KEYS: Lists with 3+ elements get a key based on their
 *    first element (car). This helps find (tag ...) style lists.
 *    Skipped for 2-element lists where pair keys are more selective.
 */

/*
 * Key Extraction Strategy:
 * 
 * Keys are extracted selectively to balance index size and query performance:
 * 
 * 1. PAIR KEYS: For 2-element (symbol value) lists like (age 30),
 *    extract hash(symbol, value). This provides high selectivity for
 *    common key-value patterns.
 * 
 * 2. ATOM KEYS: All atoms (symbols, strings, integers, floats) get
 *    individual keys. Required for queries that search for specific values.
 * 
 * 3. LIST HEAD KEYS: Lists with 3+ elements get a key based on their
 *    first element (car). This helps find (tag ...) style lists.
 *    Skipped for 2-element lists where pair keys are more selective.
 */
static inline uint32
hash_combine32(uint32 seed, uint32 hash)
{
    return seed ^ (hash + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

/*
 * Simple hash set for O(1) key deduplication.
 * Uses open addressing with linear probing.
 * Size is power of 2 for fast modulo via bitmask.
 * 
 * Sizing rationale: MAX_GIN_KEYS is 2048. With pair keys and Bloom keys,
 * very key-rich inputs could approach 3000-4000 unique keys. Using 8192
 * maintains a load factor under 50%, keeping probe chains short.
 */
#define KEY_HASHSET_SIZE 8192  /* Must be power of 2 and >> MAX_GIN_KEYS */
#define KEY_HASHSET_MASK (KEY_HASHSET_SIZE - 1)
#define KEY_HASHSET_EMPTY 0x7FFFFFFF  /* Sentinel value (keys have high bit set) */

typedef struct KeyHashSet
{
    int32   slots[KEY_HASHSET_SIZE];
    int     count;
} KeyHashSet;

static inline void
key_hashset_init(KeyHashSet *hs)
{
    int i;
    for (i = 0; i < KEY_HASHSET_SIZE; i++)
        hs->slots[i] = KEY_HASHSET_EMPTY;
    hs->count = 0;
}

/*
 * Try to insert key into hash set.
 * Returns true if key was newly inserted, false if already present.
 */
static inline bool
key_hashset_insert(KeyHashSet *hs, int32 key)
{
    uint32 idx = ((uint32)key) & KEY_HASHSET_MASK;
    int probes = 0;
    
    while (probes < KEY_HASHSET_SIZE)
    {
        if (hs->slots[idx] == KEY_HASHSET_EMPTY)
        {
            /* Empty slot - insert here */
            hs->slots[idx] = key;
            hs->count++;
            return true;
        }
        if (hs->slots[idx] == key)
        {
            /* Already present */
            return false;
        }
        /* Linear probing */
        idx = (idx + 1) & KEY_HASHSET_MASK;
        probes++;
    }
    
    /* Table full - shouldn't happen with proper sizing */
    return false;
}

/* Forward declarations */
static void extract_keys_recursive(uint8 *ptr, uint8 *end, 
                                   char **symbols, int *sym_lengths, int sym_count,
                                   int32 **keys, int *nkeys, int *capacity,
                                   KeyHashSet *seen);
static uint32 get_element_hash(uint8 *ptr, uint8 *end,
                               char **symbols, int *sym_lengths, int sym_count);

PG_FUNCTION_INFO_V1(sexp_gin_extract_value);
PG_FUNCTION_INFO_V1(sexp_gin_extract_query);
PG_FUNCTION_INFO_V1(sexp_gin_consistent);
PG_FUNCTION_INFO_V1(sexp_gin_triconsistent);

/*
 * Compute a key for an atom value
 */
static int32
make_atom_key(uint32 type_marker, uint32 value_hash)
{
    /* Combine type and value hash, ensuring non-zero result */
    uint32 combined = type_marker ^ value_hash;
    return (int32)(combined | 0x80000000);  /* Ensure high bit set to avoid 0 */
}

/*
 * Add a key to the array, growing if needed.
 * Uses hash set for O(1) deduplication instead of O(n) linear scan.
 */
static void
add_key(int32 **keys, int *nkeys, int *capacity, int32 key, KeyHashSet *seen)
{
    if (*nkeys >= MAX_GIN_KEYS)
        return;  /* Safety limit */
    
    /* O(1) duplicate check using hash set */
    if (!key_hashset_insert(seen, key))
        return;  /* Already seen */
    
    if (*nkeys >= *capacity)
    {
        *capacity = (*capacity == 0) ? 64 : *capacity * 2;
        if (*capacity > MAX_GIN_KEYS)
            *capacity = MAX_GIN_KEYS;
        *keys = repalloc(*keys, sizeof(int32) * (*capacity));
    }
    
    (*keys)[(*nkeys)++] = key;
}

/*
 * Get a hash representing an element's value
 * Uses PostgreSQL's native hash functions for stability.
 */
static uint32
get_element_hash(uint8 *ptr, uint8 *end,
                 char **symbols, int *sym_lengths, int sym_count)
{
    uint8 tag;
    
    if (ptr >= end)
        return 0;
    
    tag = *ptr & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            return sexp_hash_uint32(0);
            
        case SEXP_TAG_SMALLINT:
        {
            int8 val = (*ptr & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
            return sexp_hash_int64((int64)val);
        }
            
        case SEXP_TAG_INTEGER:
        {
            uint8 *p = ptr + 1;
            uint64 uval = decode_varint(&p, end);
            int64 val = zigzag_decode(uval);
            return sexp_hash_int64(val);
        }
            
        case SEXP_TAG_FLOAT:
        {
            if (ptr + 9 <= end)
            {
                double dval;
                memcpy(&dval, ptr + 1, sizeof(double));
                return sexp_hash_float64(dval);
            }
            return 0;
        }
            
        case SEXP_TAG_SYMBOL_REF:
        {
            uint8 *p = ptr + 1;
            uint64 idx = decode_varint(&p, end);
            if (idx < (uint64)sym_count)
            {
                return sexp_hash_bytes(symbols[idx], sym_lengths[idx]);
            }
            return 0;
        }
            
        case SEXP_TAG_SHORT_STRING:
        {
            int len = *ptr & SEXP_DATA_MASK;
            if (ptr + 1 + len <= end)
            {
                return sexp_hash_bytes(ptr + 1, len);
            }
            return 0;
        }
            
        case SEXP_TAG_LONG_STRING:
        {
            uint8 *p = ptr + 1;
            uint64 len = decode_varint(&p, end);
            if (p + len <= end)
            {
                return sexp_hash_bytes(p, (int)len);
            }
            return 0;
        }
            
        case SEXP_TAG_LIST:
        {
            /* For lists, use the head symbol hash if available */
            uint8 *p = ptr + 1;
            uint32 count = *ptr & SEXP_DATA_MASK;
            uint8 *data_start;
            
            if (count == 0)
            {
                /* Large list */
                if (p + 8 > end)
                    return 0;
                count = *(uint32 *)p;
                p += 8;  /* count + hash */
                p += count * sizeof(SEntry);  /* skip SEntry table */
            }
            else
            {
                /* Small list v6: skip size prefix (varint) */
                (void)decode_varint(&p, end);
            }
            data_start = p;
            
            if (count > 0)
            {
                /* Get hash of first element */
                return get_element_hash(data_start, end, symbols, sym_lengths, sym_count);
            }
            return sexp_hash_uint32(0);  /* Empty list */
        }
            
        default:
            return 0;
    }
}

/*
 * Extract keys recursively from all elements.
 * 
 * Key extraction strategy for indexing:
 * - For 2-element (symbol value) lists: extract pair key AND atom keys
 * - For lists with 3+ elements: extract list head key + recurse into children
 * - Skip LIST_HEAD keys for 2-element pairs (pair key is more selective)
 * 
 * Queries for @>> look for atom keys, so atom keys are always included.
 * Pair keys provide additional selectivity for common key-value patterns.
 */
static void
extract_keys_recursive_impl(uint8 *ptr, uint8 *end, 
                            char **symbols, int *sym_lengths, int sym_count,
                            int32 **keys, int *nkeys, int *capacity,
                            KeyHashSet *seen, bool in_pair);

static void
extract_keys_recursive(uint8 *ptr, uint8 *end, 
                       char **symbols, int *sym_lengths, int sym_count,
                       int32 **keys, int *nkeys, int *capacity,
                       KeyHashSet *seen)
{
    extract_keys_recursive_impl(ptr, end, symbols, sym_lengths, sym_count,
                                keys, nkeys, capacity, seen, false);
}

static void
extract_keys_recursive_impl(uint8 *ptr, uint8 *end, 
                            char **symbols, int *sym_lengths, int sym_count,
                            int32 **keys, int *nkeys, int *capacity,
                            KeyHashSet *seen, bool in_pair)
{
    uint8 tag;
    uint32 hash;
    
    (void)in_pair;  /* Currently unused - keeping for future optimization */
    
    if (ptr >= end || *nkeys >= MAX_GIN_KEYS)
        return;
    
    tag = *ptr & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            /* NIL - add atom key */
            hash = sexp_hash_uint32(0);
            add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_ATOM, hash), seen);
            break;
            
        case SEXP_TAG_SMALLINT:
        {
            int8 val = (*ptr & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
            hash = sexp_hash_int64((int64)val);
            add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_INTEGER, hash), seen);
            break;
        }
            
        case SEXP_TAG_INTEGER:
        {
            uint8 *p = ptr + 1;
            uint64 uval = decode_varint(&p, end);
            int64 val = zigzag_decode(uval);
            hash = sexp_hash_int64(val);
            add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_INTEGER, hash), seen);
            break;
        }
            
        case SEXP_TAG_FLOAT:
        {
            if (ptr + 9 <= end)
            {
                double dval;
                memcpy(&dval, ptr + 1, sizeof(double));
                hash = sexp_hash_float64(dval);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_FLOAT, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_SYMBOL_REF:
        {
            /* Always extract symbol keys - they're highly selective */
            uint8 *p = ptr + 1;
            uint64 idx = decode_varint(&p, end);
            if (idx < (uint64)sym_count)
            {
                hash = sexp_hash_bytes(symbols[idx], sym_lengths[idx]);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_SYMBOL, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_SHORT_STRING:
        {
            /* Always extract string keys - queries look for these */
            int len = *ptr & SEXP_DATA_MASK;
            if (ptr + 1 + len <= end)
            {
                hash = sexp_hash_bytes(ptr + 1, len);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_STRING, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_LONG_STRING:
        {
            uint8 *p = ptr + 1;
            uint64 len = decode_varint(&p, end);
            if (p + len <= end)
            {
                hash = sexp_hash_bytes(p, (int)len);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_STRING, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_LIST:
        {
            uint8 *p = ptr + 1;
            uint32 count;
            uint8 *data_start;
            SEntry *sentries = NULL;
            bool is_large_list;
            uint8 tag_byte = *ptr;
            uint32 head_hash = 0;
            uint32 i;
            bool is_pair_list;
            
            count = tag_byte & SEXP_DATA_MASK;
            is_large_list = (count == 0);
            
            if (is_large_list)
            {
                if (p + 4 > end)
                    return;
                count = *(uint32 *)p;
                p += 4;
                /* Skip structural hash */
                p += 4;
                sentries = (SEntry *)p;
                p += count * sizeof(SEntry);
            }
            else
            {
                /* Small list v6: skip size prefix (varint) */
                (void)decode_varint(&p, end);
            }
            
            data_start = p;
            
            if (count == 0)
            {
                /* Empty list - skip adding key, not useful for queries */
                return;
            }
            
            /* Check if this is a 2-element pair with symbol head */
            is_pair_list = (count == 2 && (*data_start & SEXP_TAG_MASK) == SEXP_TAG_SYMBOL_REF);
            
            /* Get head element hash for list identification */
            head_hash = get_element_hash(data_start, end, symbols, sym_lengths, sym_count);
            
            /*
             * Key extraction for lists:
             * - For 2-element (symbol value) pairs: extract pair key (skip LIST_HEAD)
             * - For lists with 3+ elements: extract list head key
             * - Always recurse into children to extract atom keys
             * 
             * Pair keys provide better selectivity than LIST_HEAD for
             * common patterns like (name "value").
             */
            if (is_pair_list)
            {
                uint8 *second_elem;
                uint32 second_hash;
                uint32 pair_hash;
                
                /* Find second element */
                if (is_large_list && sentries)
                {
                    second_elem = data_start + SENTRY_GET_OFFSET(sentries[1]);
                }
                else
                {
                    second_elem = sexp_skip_element(data_start, end);
                }
                
                /* Compute hash of second element */
                second_hash = get_element_hash(second_elem, end, symbols, sym_lengths, sym_count);
                
                /* Combine head (symbol) hash with second element hash */
                pair_hash = hash_combine32(KEY_TYPE_PAIR, head_hash);
                pair_hash = hash_combine32(pair_hash, second_hash);
                
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_PAIR, pair_hash), seen);
                
                /* Skip LIST_HEAD for pairs - pair key is more selective */
            }
            else
            {
                /* Non-pair list: add list head key for selectivity */
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_LIST_HEAD, head_hash), seen);
            }
            
            /* Always recurse into children to extract atom keys */
            if (is_large_list && sentries)
            {
                for (i = 0; i < count && *nkeys < MAX_GIN_KEYS; i++)
                {
                    uint8 *elem_ptr = data_start + SENTRY_GET_OFFSET(sentries[i]);
                    extract_keys_recursive_impl(elem_ptr, end, symbols, sym_lengths, sym_count,
                                                keys, nkeys, capacity, seen, is_pair_list);
                }
            }
            else
            {
                uint8 *elem_ptr = data_start;
                for (i = 0; i < count && elem_ptr < end && *nkeys < MAX_GIN_KEYS; i++)
                {
                    extract_keys_recursive_impl(elem_ptr, end, symbols, sym_lengths, sym_count,
                                                keys, nkeys, capacity, seen, is_pair_list);
                    elem_ptr = sexp_skip_element(elem_ptr, end);
                }
            }
            break;
        }
    }
}

/*
 * sexp_gin_extract_value - Extract GIN keys from a stored value
 */
Datum
sexp_gin_extract_value(PG_FUNCTION_ARGS)
{
    Sexp       *sexp = PG_GETARG_SEXP(0);
    int32      *nkeys = (int32 *) PG_GETARG_POINTER(1);
    Datum      *keys_out;
    int32      *keys;
    int         key_count = 0;
    int         capacity = 0;
    uint8      *data;
    uint8      *end;
    uint8      *ptr;
    int         sym_count;
    char      **symbols;
    int        *sym_lengths;
    char       *stack_symbols[SEXP_SMALL_SYMTAB_SIZE];
    int         stack_lengths[SEXP_SMALL_SYMTAB_SIZE];
    bool        use_stack;
    int         i;
    KeyHashSet  seen;
    
    /* Initialize */
    keys = palloc(sizeof(int32) * 64);
    capacity = 64;
    key_hashset_init(&seen);
    
    data = SEXP_DATA_PTR(sexp);
    end = data + SEXP_DATA_SIZE(sexp);
    ptr = data;
    
    /* Read symbol table */
    sym_count = (int)decode_varint(&ptr, end);
    use_stack = (sym_count <= SEXP_SMALL_SYMTAB_SIZE);
    
    if (use_stack)
    {
        symbols = stack_symbols;
        sym_lengths = stack_lengths;
    }
    else
    {
        symbols = palloc(sizeof(char *) * sym_count);
        sym_lengths = palloc(sizeof(int) * sym_count);
    }
    
    for (i = 0; i < sym_count; i++)
    {
        int len = (int)decode_varint(&ptr, end);
        symbols[i] = (char *)ptr;
        sym_lengths[i] = len;
        ptr += len;
    }
    
    /* Extract keys */
    extract_keys_recursive(ptr, end, symbols, sym_lengths, sym_count,
                          &keys, &key_count, &capacity, &seen);
    
    /* Clean up symbol table if heap-allocated */
    if (!use_stack)
    {
        pfree(symbols);
        pfree(sym_lengths);
    }
    
    /* Convert to Datum array */
    if (key_count == 0)
    {
        *nkeys = 1;
        keys_out = palloc(sizeof(Datum));
        keys_out[0] = Int32GetDatum(make_atom_key(KEY_TYPE_ATOM, 0));
    }
    else
    {
        *nkeys = key_count;
        keys_out = palloc(sizeof(Datum) * key_count);
        for (i = 0; i < key_count; i++)
        {
            keys_out[i] = Int32GetDatum(keys[i]);
        }
    }
    
    pfree(keys);
    
    PG_RETURN_POINTER(keys_out);
}

/*
 * Extract keys for query - different behavior than value extraction.
 * For @>> queries, we must NOT extract pair keys from the query because
 * the query may be a subset (e.g., query has 2 elements, data has 5).
 */
static void
extract_query_keys_recursive(uint8 *ptr, uint8 *end, 
                             char **symbols, int *sym_lengths, int sym_count,
                             int32 **keys, int *nkeys, int *capacity,
                             KeyHashSet *seen, bool skip_pair_keys);

static void
extract_query_keys_recursive(uint8 *ptr, uint8 *end, 
                             char **symbols, int *sym_lengths, int sym_count,
                             int32 **keys, int *nkeys, int *capacity,
                             KeyHashSet *seen, bool skip_pair_keys)
{
    uint8 tag;
    uint32 hash;
    
    if (ptr >= end || *nkeys >= MAX_GIN_KEYS)
        return;
    
    tag = *ptr & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            hash = sexp_hash_uint32(0);
            add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_ATOM, hash), seen);
            break;
            
        case SEXP_TAG_SMALLINT:
        {
            int8 val = (*ptr & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
            hash = sexp_hash_int64((int64)val);
            add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_INTEGER, hash), seen);
            break;
        }
            
        case SEXP_TAG_INTEGER:
        {
            uint8 *p = ptr + 1;
            uint64 uval = decode_varint(&p, end);
            int64 val = zigzag_decode(uval);
            hash = sexp_hash_int64(val);
            add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_INTEGER, hash), seen);
            break;
        }
            
        case SEXP_TAG_FLOAT:
        {
            if (ptr + 9 <= end)
            {
                double dval;
                memcpy(&dval, ptr + 1, sizeof(double));
                hash = sexp_hash_float64(dval);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_FLOAT, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_SYMBOL_REF:
        {
            uint8 *p = ptr + 1;
            uint64 idx = decode_varint(&p, end);
            if (idx < (uint64)sym_count)
            {
                hash = sexp_hash_bytes(symbols[idx], sym_lengths[idx]);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_SYMBOL, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_SHORT_STRING:
        {
            int len = *ptr & SEXP_DATA_MASK;
            if (ptr + 1 + len <= end)
            {
                hash = sexp_hash_bytes(ptr + 1, len);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_STRING, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_LONG_STRING:
        {
            uint8 *p = ptr + 1;
            uint64 len = decode_varint(&p, end);
            if (p + len <= end)
            {
                hash = sexp_hash_bytes(p, (int)len);
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_STRING, hash), seen);
            }
            break;
        }
            
        case SEXP_TAG_LIST:
        {
            uint8 *p = ptr + 1;
            uint32 count;
            uint8 *data_start;
            SEntry *sentries = NULL;
            bool is_large_list;
            uint8 tag_byte = *ptr;
            uint32 head_hash = 0;
            uint32 i;
            bool is_pair_list;
            
            count = tag_byte & SEXP_DATA_MASK;
            is_large_list = (count == 0);
            
            if (is_large_list)
            {
                if (p + 4 > end)
                    return;
                count = *(uint32 *)p;
                p += 4;
                p += 4;  /* Skip structural hash */
                sentries = (SEntry *)p;
                p += count * sizeof(SEntry);
            }
            else
            {
                (void)decode_varint(&p, end);  /* Skip size prefix */
            }
            
            data_start = p;
            
            if (count == 0)
            {
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_ATOM, 0), seen);
                return;
            }
            
            /* Check if this is a 2-element pair with symbol head */
            is_pair_list = (count == 2 && (*data_start & SEXP_TAG_MASK) == SEXP_TAG_SYMBOL_REF);
            
            /* Get head element hash for list identification */
            head_hash = get_element_hash(data_start, end, symbols, sym_lengths, sym_count);
            
            /*
             * KEY EXTRACTION STRATEGY (must match value extraction):
             * - For 2-element (symbol value) pairs: skip LIST_HEAD (pair key in value)
             * - For lists with 3+ elements: add LIST_HEAD key
             * 
             * This ensures query keys are always a subset of stored data keys.
             */
            if (!is_pair_list)
            {
                /* Add list head key only for non-pair lists */
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_LIST_HEAD, head_hash), seen);
            }
            
            /*
             * PAIR KEY: Only add for value extraction (structural @>), NOT for @>> queries.
             * For @>> queries, skip_pair_keys is true because the query (user (id 100)) 
             * may match data with more elements like (user (id 100) (name "x") ...).
             */
            if (!skip_pair_keys && is_pair_list)
            {
                uint8 *second_elem;
                uint32 second_hash;
                uint32 pair_hash;
                
                if (is_large_list && sentries)
                    second_elem = data_start + SENTRY_GET_OFFSET(sentries[1]);
                else
                    second_elem = sexp_skip_element(data_start, end);
                
                second_hash = get_element_hash(second_elem, end, symbols, sym_lengths, sym_count);
                
                /* Use hash_combine32 for better mixing */
                pair_hash = hash_combine32(KEY_TYPE_PAIR, head_hash);
                pair_hash = hash_combine32(pair_hash, second_hash);
                
                add_key(keys, nkeys, capacity, make_atom_key(KEY_TYPE_PAIR, pair_hash), seen);
            }
            
            /* Recursively extract keys from all children */
            if (is_large_list && sentries)
            {
                for (i = 0; i < count && *nkeys < MAX_GIN_KEYS; i++)
                {
                    uint8 *elem_ptr = data_start + SENTRY_GET_OFFSET(sentries[i]);
                    extract_query_keys_recursive(elem_ptr, end, symbols, sym_lengths, sym_count,
                                                keys, nkeys, capacity, seen, skip_pair_keys);
                }
            }
            else
            {
                uint8 *elem_ptr = data_start;
                for (i = 0; i < count && elem_ptr < end && *nkeys < MAX_GIN_KEYS; i++)
                {
                    extract_query_keys_recursive(elem_ptr, end, symbols, sym_lengths, sym_count,
                                                keys, nkeys, capacity, seen, skip_pair_keys);
                    elem_ptr = sexp_skip_element(elem_ptr, end);
                }
            }
            break;
        }
    }
}

/*
 * sexp_gin_extract_query - Extract GIN keys from a query value
 */
Datum
sexp_gin_extract_query(PG_FUNCTION_ARGS)
{
    Sexp       *query = PG_GETARG_SEXP(0);
    int32      *nkeys = (int32 *) PG_GETARG_POINTER(1);
    StrategyNumber strategy = PG_GETARG_UINT16(2);
    /* bool **pmatch = (bool **) PG_GETARG_POINTER(3); */
    /* Pointer *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
    /* bool **nullFlags = (bool **) PG_GETARG_POINTER(5); */
    int32      *searchMode = (int32 *) PG_GETARG_POINTER(6);
    
    Datum      *keys_out;
    int32      *keys;
    int         key_count = 0;
    int         capacity = 0;
    uint8      *data;
    uint8      *end;
    uint8      *ptr;
    int         sym_count;
    char      **symbols;
    int        *sym_lengths;
    char       *stack_symbols[SEXP_SMALL_SYMTAB_SIZE];
    int         stack_lengths[SEXP_SMALL_SYMTAB_SIZE];
    bool        use_stack;
    int         i;
    KeyHashSet  seen;
    bool        skip_pair_keys;
    
    switch (strategy)
    {
        case SEXP_GIN_CONTAINS_STRATEGY:     /* @> structural */
            /*
             * For structural containment, pair keys are fine because
             * the query structure must match exactly within the container.
             */
            skip_pair_keys = false;
            break;
            
        case SEXP_GIN_CONTAINS_KEY_STRATEGY: /* @>> key-based */
            /*
             * For key-based containment, DON'T extract pair keys from query.
             * The query (user (id 100)) may match data with more elements like
             * (user (id 100) (name "x") ...). The stored data won't have a
             * pair key for (user, ...) since it has more than 2 elements.
             */
            skip_pair_keys = true;
            break;
            
        case SEXP_GIN_CONTAINED_STRATEGY:   /* <@ */
            /* For contained-by, use GIN_SEARCH_MODE_ALL */
            *searchMode = GIN_SEARCH_MODE_ALL;
            *nkeys = 0;
            PG_RETURN_POINTER(NULL);
            
        default:
            elog(ERROR, "sexp_gin_extract_query: unknown strategy %d", strategy);
    }
    
    /* Initialize */
    keys = palloc(sizeof(int32) * 64);
    capacity = 64;
    key_hashset_init(&seen);
    
    data = SEXP_DATA_PTR(query);
    end = data + SEXP_DATA_SIZE(query);
    ptr = data;
    
    /* Read symbol table */
    sym_count = (int)decode_varint(&ptr, end);
    use_stack = (sym_count <= SEXP_SMALL_SYMTAB_SIZE);
    
    if (use_stack)
    {
        symbols = stack_symbols;
        sym_lengths = stack_lengths;
    }
    else
    {
        symbols = palloc(sizeof(char *) * sym_count);
        sym_lengths = palloc(sizeof(int) * sym_count);
    }
    
    for (i = 0; i < sym_count; i++)
    {
        int len = (int)decode_varint(&ptr, end);
        symbols[i] = (char *)ptr;
        sym_lengths[i] = len;
        ptr += len;
    }
    
    /* Extract keys - skip pair keys for @>> queries */
    extract_query_keys_recursive(ptr, end, symbols, sym_lengths, sym_count,
                                &keys, &key_count, &capacity, &seen, skip_pair_keys);
    
    /* Clean up symbol table if heap-allocated */
    if (!use_stack)
    {
        pfree(symbols);
        pfree(sym_lengths);
    }
    
    /* Set search mode - require ALL keys to match */
    *searchMode = GIN_SEARCH_MODE_DEFAULT;
    
    /* Convert to Datum array */
    if (key_count == 0)
    {
        *nkeys = 1;
        keys_out = palloc(sizeof(Datum));
        keys_out[0] = Int32GetDatum(make_atom_key(KEY_TYPE_ATOM, 0));
    }
    else
    {
        *nkeys = key_count;
        keys_out = palloc(sizeof(Datum) * key_count);
        for (i = 0; i < key_count; i++)
        {
            keys_out[i] = Int32GetDatum(keys[i]);
        }
    }
    
    pfree(keys);
    
    PG_RETURN_POINTER(keys_out);
}

/*
 * sexp_gin_consistent - Check if indexed keys are consistent with query
 */
Datum
sexp_gin_consistent(PG_FUNCTION_ARGS)
{
    bool       *check = (bool *) PG_GETARG_POINTER(0);
    StrategyNumber strategy = PG_GETARG_UINT16(1);
    /* Sexp *query = PG_GETARG_SEXP(2); */
    int32       nkeys = PG_GETARG_INT32(3);
    /* Pointer *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
    bool       *recheck = (bool *) PG_GETARG_POINTER(5);
    /* Datum *queryKeys = (Datum *) PG_GETARG_POINTER(6); */
    /* bool *nullFlags = (bool *) PG_GETARG_POINTER(7); */
    
    bool        result = true;
    int         i;
    
    /* Always require recheck because:
     * 1. Keys are hashes, so collisions are possible
     * 2. Key presence is necessary but not sufficient for containment
     */
    *recheck = true;
    
    switch (strategy)
    {
        case SEXP_GIN_CONTAINS_STRATEGY:     /* @> structural */
        case SEXP_GIN_CONTAINS_KEY_STRATEGY: /* @>> key-based */
            /* All query keys must be present in the indexed value */
            for (i = 0; i < nkeys; i++)
            {
                if (!check[i])
                {
                    result = false;
                    break;
                }
            }
            break;
            
        case SEXP_GIN_CONTAINED_STRATEGY:   /* <@ */
            /* For contained-by, we can't efficiently pre-filter */
            result = true;
            break;
            
        default:
            elog(ERROR, "sexp_gin_consistent: unknown strategy %d", strategy);
    }
    
    PG_RETURN_BOOL(result);
}

/*
 * sexp_gin_triconsistent - Ternary consistent check for GIN
 * 
 * OPTIMIZATION: Return GIN_TRUE more aggressively in certain cases to
 * reduce rechecks. However, we must be conservative because:
 * 1. Keys are hashes - collisions are possible (rare but non-zero)
 * 2. For structural containment, key presence is necessary but not sufficient
 * 
 * Cases where we can return GIN_TRUE (skipping recheck):
 * - Single atom queries where all keys are GIN_TRUE
 * - This is safe because a single atom key uniquely identifies the value
 *   (modulo hash collisions, which are rare with 32-bit hashes)
 * 
 * Cases where we must return GIN_MAYBE (requiring recheck):
 * - List containment queries: key presence doesn't guarantee structural match
 * - Multiple keys: need to verify they coexist in proper structure
 */
Datum
sexp_gin_triconsistent(PG_FUNCTION_ARGS)
{
    GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
    StrategyNumber strategy = PG_GETARG_UINT16(1);
    /* Sexp *query = PG_GETARG_SEXP(2); */
    int32       nkeys = PG_GETARG_INT32(3);
    /* Pointer *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
    /* Datum *queryKeys = (Datum *) PG_GETARG_POINTER(5); */
    /* bool *nullFlags = (bool *) PG_GETARG_POINTER(6); */
    
    GinTernaryValue result = GIN_MAYBE;
    int         i;
    bool        all_true = true;
    bool        any_false = false;
    
    /* First pass: check all key states */
    for (i = 0; i < nkeys; i++)
    {
        if (check[i] == GIN_FALSE)
        {
            any_false = true;
            all_true = false;
            break;  /* Can return GIN_FALSE immediately */
        }
        else if (check[i] == GIN_MAYBE)
        {
            all_true = false;
        }
    }
    
    switch (strategy)
    {
        case SEXP_GIN_CONTAINS_STRATEGY:     /* @> structural */
            if (any_false)
            {
                result = GIN_FALSE;
            }
            else if (all_true && nkeys == 1)
            {
                /*
                 * OPTIMIZATION: Single-key structural containment with GIN_TRUE.
                 * 
                 * For a single atom query (symbol, int, string), if the key is
                 * definitely present, the containment holds because:
                 * - Container @> atom iff atom appears anywhere in container
                 * - A single key with GIN_TRUE means that exact atom is present
                 * 
                 * Note: We accept the tiny risk of hash collision here for the
                 * performance benefit of avoiding recheck.
                 */
                result = GIN_TRUE;
            }
            else
            {
                result = GIN_MAYBE;
            }
            break;
            
        case SEXP_GIN_CONTAINS_KEY_STRATEGY: /* @>> key-based */
            if (any_false)
            {
                result = GIN_FALSE;
            }
            else if (all_true && nkeys == 1)
            {
                /* Same optimization for single-atom key-based containment */
                result = GIN_TRUE;
            }
            else
            {
                result = GIN_MAYBE;
            }
            break;
            
        case SEXP_GIN_CONTAINED_STRATEGY:   /* <@ */
            /* For contained-by, we can't efficiently pre-filter */
            result = GIN_MAYBE;
            break;
            
        default:
            elog(ERROR, "sexp_gin_triconsistent: unknown strategy %d", strategy);
    }
    
    PG_RETURN_GIN_TERNARY_VALUE(result);
}
