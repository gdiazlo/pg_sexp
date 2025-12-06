/*
 * pg_sexp.h
 *
 * S-expression data type for PostgreSQL
 * Similar to jsonb but for s-expressions (Lisp-like syntax)
 *
 * S-expression grammar:
 *   sexp     ::= atom | list
 *   atom     ::= symbol | string | number
 *   list     ::= '(' sexp* ')'
 *   symbol   ::= [a-zA-Z_][a-zA-Z0-9_-]*
 *   string   ::= '"' (escaped_char | [^"])* '"'
 *   number   ::= integer | float
 *
 * OPTIMIZED BINARY FORMAT:
 * ========================
 *
 * Root structure:
 *   [1 byte: version]                  -- SEXP_FORMAT_VERSION
 *   [varint: symbol_count]
 *   [symbol_table: symbol_count entries]
 *   [root_element]
 *
 * Symbol table entry:
 *   [varint: length]
 *   [bytes: symbol chars]
 *
 * Element encoding (first byte determines type):
 *   Bits 7-5: Type tag (3 bits)
 *   Bits 4-0: Inline data (count, small int value, or string length)
 *
 * Type tags (3 bits):
 *   000 (0x00) = NIL
 *   001 (0x20) = Small integer (-16 to 15 in bits 4-0 with bias)
 *   010 (0x40) = Integer (zigzag-encoded varint follows)
 *   011 (0x60) = Float (8 bytes IEEE 754 double follows)
 *   100 (0x80) = Symbol reference (varint symbol index follows)
 *   101 (0xA0) = Short string (length 0-31 in bits 4-0, then bytes)
 *   110 (0xC0) = Long string (varint length follows, then bytes)
 *   111 (0xE0) = List (see below for small vs large format)
 *
 * LIST FORMAT:
 * ============
 * Lists use two formats based on element count for optimal space/speed tradeoff.
 *
 * SMALL LIST (count 1-4, inline count in tag bits 4-0):
 *   [0xE0 | count]                     -- 1 byte: tag + count
 *   [varint: payload_size]             -- total size of element data (for O(1) skip)
 *   [element_0]...[element_N-1]        -- sequential element data
 *
 * LARGE LIST (count 0 in tag bits signals large format):
 *   [0xE0]                             -- 1 byte: tag with count=0
 *   [uint32: count]                    -- 4 bytes: element count
 *   [uint32: structural_hash]          -- 4 bytes: combined hash for quick rejection
 *   [SEntry[0]..SEntry[count-1]]       -- 4*count bytes: type+offset table
 *   [element_0]...[element_N-1]        -- element data
 *
 * NIL (empty list) is encoded as SEXP_TAG_NIL (0x00), not as a list.
 *
 * SEntry (32-bit, like jsonb's JEntry):
 *   Bits 31-29: Type tag (for type filtering without dereferencing)
 *   Bits 28-0:  Offset from start of element data (max 512MB)
 *
 * PERFORMANCE CHARACTERISTICS:
 * - O(1) random access: nth/car use SEntry offsets for large lists
 * - O(1) skip: Small lists have payload size, large lists use SEntry
 * - O(1) type check: SEntry embeds type, no need to read element byte
 * - Hash rejection: Large list hash enables quick containment rejection
 *
 * Varint encoding (protobuf-style):
 *   - Each byte: 7 bits data, high bit = continuation flag
 *   - Little-endian order
 *   - Fast path for single-byte values (0-127)
 */

#ifndef PG_SEXP_H
#define PG_SEXP_H

#include "postgres.h"
#include "fmgr.h"
#include "varatt.h"
#include "lib/stringinfo.h"
#include "common/hashfn.h"

/* Binary format version - small lists include size prefix for O(1) skipping */
#define SEXP_FORMAT_VERSION 6

/* Type tags (3 bits, stored in bits 7-5 for atoms, or in SEntry for list elements) */
#define SEXP_TAG_NIL          0x00  /* 000 */
#define SEXP_TAG_SMALLINT     0x20  /* 001 */
#define SEXP_TAG_INTEGER      0x40  /* 010 */
#define SEXP_TAG_FLOAT        0x60  /* 011 */
#define SEXP_TAG_SYMBOL_REF   0x80  /* 100 */
#define SEXP_TAG_SHORT_STRING 0xA0  /* 101 */
#define SEXP_TAG_LONG_STRING  0xC0  /* 110 */
#define SEXP_TAG_LIST         0xE0  /* 111 - list with SEntry table */

#define SEXP_TAG_MASK         0xE0  /* Top 3 bits */
#define SEXP_DATA_MASK        0x1F  /* Bottom 5 bits */

/* 
 * Small list threshold - lists with count <= this use compact inline format.
 * Lists with count > this use the large format with SEntry table.
 * 
 * Set to 4 to balance:
 * - Space: Small lists save 4*count bytes (no SEntry table)
 * - Speed: Large lists enable O(1) nth access and type filtering
 * 
 * Small list format: [tag|count][varint:payload_size][elements...]
 * Large list format: [tag][uint32:count][uint32:hash][SEntry table][elements...]
 */
#define SEXP_SMALL_LIST_MAX   4

/*
 * Structural Hash for Quick Containment Rejection
 * ================================================
 * Large lists store a 32-bit structural hash computed from all children.
 * This enables quick rejection in containment checks: if hashes don't
 * match in expected ways, we can skip expensive recursive comparison.
 *
 * Hash computation uses PostgreSQL's native hash_any() / hash_bytes()
 * functions from common/hashfn.h for stability and consistency with
 * PostgreSQL's internal hashing (e.g., JSONB GIN indexes).
 *
 * Hash values:
 *   - NIL:     0
 *   - Symbol:  hash_bytes(SYMBOL_TAG + symbol_string)
 *   - String:  hash_bytes(STRING_TAG + string_content)
 *   - Integer: hash_bytes_uint32(value)
 *   - Float:   hash_bytes(value_bits)
 *   - List:    hash_combine(hash_bytes(LIST_TAG + count), child_hashes)
 *
 * The hash is stored in large lists after count, before the SEntry table.
 * For atoms and small lists, hash is computed on-demand (cheap).
 */

/*
 * SEntry - S-expression Entry (like jsonb's JEntry)
 * 
 * 32-bit format packs type + offset into one read:
 *   Bits 31-29: Type tag (3 bits) - element type
 *   Bit 28:     Reserved (for future has_offset flag like jsonb)
 *   Bits 27-0:  Offset (28 bits) - offset from data start, max 256MB
 *
 * This eliminates indirection: reading SEntry[N] gives both type AND location.
 */
typedef uint32 SEntry;

#define SENTRY_TYPE_SHIFT     29
#define SENTRY_TYPE_MASK      0xE0000000  /* Top 3 bits */
#define SENTRY_OFFSET_MASK    0x0FFFFFFF  /* Bottom 28 bits */

/* SEntry type values (shifted to top 3 bits) */
#define SENTRY_TYPE_NIL       (0 << SENTRY_TYPE_SHIFT)
#define SENTRY_TYPE_INTEGER   (1 << SENTRY_TYPE_SHIFT)  /* covers smallint too */
#define SENTRY_TYPE_FLOAT     (2 << SENTRY_TYPE_SHIFT)
#define SENTRY_TYPE_SYMBOL    (3 << SENTRY_TYPE_SHIFT)
#define SENTRY_TYPE_STRING    (4 << SENTRY_TYPE_SHIFT)
#define SENTRY_TYPE_LIST      (5 << SENTRY_TYPE_SHIFT)

/* SEntry macros */
#define SENTRY_GET_TYPE(se)   ((se) & SENTRY_TYPE_MASK)
#define SENTRY_GET_OFFSET(se) ((se) & SENTRY_OFFSET_MASK)
#define SENTRY_MAKE(type, offset) ((type) | ((offset) & SENTRY_OFFSET_MASK))

/* Type checks */
#define SENTRY_IS_NIL(se)     (SENTRY_GET_TYPE(se) == SENTRY_TYPE_NIL)
#define SENTRY_IS_INTEGER(se) (SENTRY_GET_TYPE(se) == SENTRY_TYPE_INTEGER)
#define SENTRY_IS_FLOAT(se)   (SENTRY_GET_TYPE(se) == SENTRY_TYPE_FLOAT)
#define SENTRY_IS_SYMBOL(se)  (SENTRY_GET_TYPE(se) == SENTRY_TYPE_SYMBOL)
#define SENTRY_IS_STRING(se)  (SENTRY_GET_TYPE(se) == SENTRY_TYPE_STRING)
#define SENTRY_IS_LIST(se)    (SENTRY_GET_TYPE(se) == SENTRY_TYPE_LIST)

/* Small integer range: -16 to 15 (5-bit signed) */
#define SEXP_SMALLINT_MIN     (-16)
#define SEXP_SMALLINT_MAX     15
#define SEXP_SMALLINT_BIAS    16    /* Add to convert signed to unsigned 0-31 */

/* Short string max length (5 bits = 0-31) */
#define SEXP_SHORT_STRING_MAX 31

/*
 * OPTIMIZATION: Unaligned load for x86/x86_64
 * 
 * x86 processors handle unaligned memory access efficiently in hardware.
 * For other architectures (ARM, etc.), we use memcpy which the compiler
 * typically optimizes well. This avoids potential alignment faults on
 * strict-alignment architectures.
 */
#if defined(__x86_64__) || defined(__i386__) || defined(_M_IX86) || defined(_M_X64)
#define SEXP_LOAD_UINT32_UNALIGNED(ptr) (*(const uint32 *)(ptr))
#else
static inline uint32
sexp_load_uint32_unaligned(const uint8 *ptr)
{
    uint32 val;
    memcpy(&val, ptr, sizeof(uint32));
    return val;
}
#define SEXP_LOAD_UINT32_UNALIGNED(ptr) sexp_load_uint32_unaligned(ptr)
#endif

/* Legacy type enum for API compatibility */
typedef enum SexpType
{
    SEXP_NIL = 0,
    SEXP_SYMBOL,
    SEXP_STRING,
    SEXP_INTEGER,
    SEXP_FLOAT,
    SEXP_LIST
} SexpType;

/*
 * Sexp is stored as a varlena with binary data
 * The first byte after varlena header contains version/flags
 */
typedef struct Sexp
{
    int32   vl_len_;    /* varlena header */
    uint8   version;    /* format version and flags */
    char    data[FLEXIBLE_ARRAY_MEMBER];
} Sexp;

#define SEXP_VERSION(s)     ((s)->version & 0x0F)
#define SEXP_DATA_PTR(s)    ((uint8 *)((s)->data))
#define SEXP_DATA_SIZE(s)   (VARSIZE(s) - VARHDRSZ - 1)

/*
 * Parse context - includes symbol table for interning
 * 
 * Uses a simple open-addressing hash table for O(1) symbol lookup
 * instead of O(n) linear search. Hash entries store index into symbols array.
 * A value of -1 indicates empty slot.
 */
#define SYMTAB_HASH_EMPTY (-1)
#define SYMTAB_INITIAL_HASH_SIZE 64  /* Must be power of 2 */

typedef struct SexpSymbolTable
{
    char      **symbols;        /* Array of symbol strings */
    int        *lengths;        /* Length of each symbol */
    uint32     *hashes;         /* Pre-computed hash of each symbol */
    int         count;          /* Number of symbols */
    int         capacity;       /* Allocated capacity for symbols array */
    /* Hash table for fast lookup */
    int        *hash_table;     /* Maps hash slot -> symbol index (-1 = empty) */
    int         hash_size;      /* Size of hash table (always power of 2) */
    int         hash_mask;      /* hash_size - 1, for fast modulo */
} SexpSymbolTable;

typedef struct SexpParseState
{
    const char      *input;     /* Input string */
    const char      *ptr;       /* Current position */
    const char      *end;       /* End of input */
    StringInfo       output;    /* Output buffer for binary data */
    SexpSymbolTable  symtab;    /* Symbol table for interning */
    int              depth;     /* Current nesting depth */
} SexpParseState;

/*
 * Read context - for decoding binary format
 * 
 * For small symbol tables (<=16 symbols), we use stack-allocated arrays
 * to avoid palloc/pfree overhead in hot paths like containment checks.
 */
#define SEXP_SMALL_SYMTAB_SIZE 16

typedef struct SexpReadState
{
    uint8       *data;          /* Binary data pointer */
    uint8       *end;           /* End of data */
    uint8       *ptr;           /* Current read position */
    char       **symbols;       /* Symbol table (decoded) */
    int         *sym_lengths;   /* Symbol lengths */
    uint32      *sym_hashes;    /* Pre-computed symbol hashes for fast comparison */
    int          sym_count;     /* Number of symbols */
    bool         use_stack;     /* True if using stack arrays */
    char        *stack_symbols[SEXP_SMALL_SYMTAB_SIZE];   /* Stack-allocated for small tables */
    int          stack_lengths[SEXP_SMALL_SYMTAB_SIZE];   /* Stack-allocated for small tables */
    uint32       stack_hashes[SEXP_SMALL_SYMTAB_SIZE];    /* Stack-allocated for small tables */
} SexpReadState;

/* Maximum nesting depth to prevent stack overflow */
#define SEXP_MAX_DEPTH 1000

/* Maximum symbol table size */
#define SEXP_MAX_SYMBOLS 65536

/*
 * Varint encoding/decoding
 * 
 * OPTIMIZATION: Most varints in practice are 1 byte (values 0-127).
 * We provide a fast-path decode that avoids the loop for this case.
 */
static inline int
encode_varint(uint8 *buf, uint64 value)
{
    int len = 0;
    while (value >= 0x80)
    {
        buf[len++] = (uint8)(value | 0x80);
        value >>= 7;
    }
    buf[len++] = (uint8)value;
    return len;
}

/*
 * Fast-path varint decode for the common 1-byte case
 * Returns decoded value, advances *ptr
 */
static inline uint64
decode_varint(uint8 **ptr, uint8 *end)
{
    uint8 *p = *ptr;
    uint8 byte;
    
    if (unlikely(p >= end))
        return 0;
    
    byte = *p++;
    
    /* Fast path: single-byte varint (values 0-127) - most common case */
    if (likely((byte & 0x80) == 0))
    {
        *ptr = p;
        return byte;
    }
    
    /* Multi-byte varint - slower path */
    {
        uint64 result = byte & 0x7F;
        int shift = 7;
        
        while (p < end)
        {
            byte = *p++;
            result |= (uint64)(byte & 0x7F) << shift;
            if (likely((byte & 0x80) == 0))
            {
                *ptr = p;
                return result;
            }
            shift += 7;
            if (unlikely(shift >= 64))
                break;  /* Overflow protection */
        }
        *ptr = p;
        return result;
    }
}

/* Get varint encoded length without encoding */
static inline int
varint_size(uint64 value)
{
    int len = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        len++;
    }
    return len;
}

/* Encode signed integer using zigzag encoding */
static inline uint64
zigzag_encode(int64 value)
{
    return (uint64)((value << 1) ^ (value >> 63));
}

static inline int64
zigzag_decode(uint64 value)
{
    return (int64)((value >> 1) ^ -(int64)(value & 1));
}

/*
 * Hash functions using PostgreSQL's native hash_any/hash_bytes
 * 
 * These wrappers provide a consistent interface for structural hashing
 * while using PostgreSQL's stable hash functions (suitable for on-disk storage).
 * 
 * Note: hash_any() returns Datum, but we use hash_bytes() which returns uint32
 * directly for cleaner code.
 */

/* Hash arbitrary bytes - uses PostgreSQL's stable hash function */
static inline uint32
sexp_hash_bytes(const void *data, int len)
{
    return hash_bytes((const unsigned char *)data, len);
}

/* Hash a string with a type tag prefix for type disambiguation */
static inline uint32
sexp_hash_string_with_tag(uint8 tag, const char *str, int len)
{
    /*
     * Combine tag and string by hashing tag first, then XOR with string hash.
     * This ensures different types with same content have different hashes.
     */
    uint32 tag_hash = hash_bytes_uint32(tag);
    uint32 str_hash = hash_bytes((const unsigned char *)str, len);
    return hash_combine(tag_hash, str_hash);
}

/* Hash a uint32 value */
static inline uint32
sexp_hash_uint32(uint32 value)
{
    return hash_bytes_uint32(value);
}

/* Hash a uint64 value */
static inline uint32
sexp_hash_uint64(uint64 value)
{
    return hash_bytes((const unsigned char *)&value, sizeof(uint64));
}

/* Hash an int64 value (for integers) */
static inline uint32
sexp_hash_int64(int64 value)
{
    return hash_bytes((const unsigned char *)&value, sizeof(int64));
}

/* Hash a double value (for floats) */
static inline uint32
sexp_hash_float64(double value)
{
    /* Handle special case: -0.0 should hash same as 0.0 */
    if (value == 0.0)
        value = 0.0;
    return hash_bytes((const unsigned char *)&value, sizeof(double));
}

/* Rotate left for combining child hashes with position dependency */
static inline uint32
rotl32(uint32 x, int r)
{
    return (x << r) | (x >> (32 - r));
}

/*
 * Combine child hash into parent hash with position mixing
 * 
 * Uses PostgreSQL's hash_combine for the base combination, with rotation
 * to make the hash order-dependent (important for list element ordering).
 */
static inline uint32
sexp_hash_combine(uint32 parent_hash, uint32 child_hash, int position)
{
    /* Rotate child hash by position to make order-dependent */
    uint32 rotated = rotl32(child_hash, position % 31);
    return hash_combine(parent_hash, rotated);
}

/*
 * ============================================================================
 * BLOOM SIGNATURE for Containment Fast Rejection
 * ============================================================================
 *
 * A 64-bit Bloom signature provides fast rejection for containment checks.
 * The signature encodes which value types and hash bits are present in a
 * structure. If (needle_bloom & ~container_bloom) != 0, the needle cannot
 * be contained in the container.
 *
 * Design choices:
 * - 64 bits: fits in a register, no heap allocation, good cache behavior
 * - k=4 hash functions: derived from element hash via rotations
 * - False positive rate: ~6% for 10 elements, acceptable for fast-path rejection
 *
 * The signature is stored in large lists alongside the structural hash.
 * For atoms and small lists, it's computed on demand (cheap operation).
 *
 * GIN INDEX INTEGRATION:
 * We add 2 summary Bloom keys to the GIN index per sexp value:
 *   - BLOOM_SUMMARY_LO: lower 32 bits as int32 key
 *   - BLOOM_SUMMARY_HI: upper 32 bits as int32 key
 * This allows triconsistent to reject without recheck when Bloom test fails.
 */

/* Number of hash functions for Bloom signature (k parameter) */
#define BLOOM_K 4

/* Bloom signature type - 64 bits for good performance and low FP rate */
typedef uint64 BloomSig;

/*
 * Compute Bloom bit positions from an element hash.
 * Uses rotation to derive k independent positions from a single hash.
 */
static inline BloomSig
bloom_compute_sig(uint32 elem_hash)
{
    BloomSig sig = 0;
    int i;
    
    for (i = 0; i < BLOOM_K; i++)
    {
        /* Derive bit position from rotated hash */
        uint32 rotated = rotl32(elem_hash, i * 8);
        int bit_pos = rotated & 63;  /* 0-63 for 64-bit bloom */
        sig |= ((BloomSig)1 << bit_pos);
    }
    
    return sig;
}

/*
 * Combine child Bloom signature into parent.
 * Simply OR the bits - Bloom filter union operation.
 */
static inline BloomSig
bloom_combine(BloomSig parent, BloomSig child)
{
    return parent | child;
}

/*
 * Check if needle's Bloom signature is a subset of container's.
 * Returns true if needle MIGHT be contained (Bloom says maybe).
 * Returns false if needle DEFINITELY NOT contained (Bloom says no).
 *
 * This is the fast-path rejection test:
 *   if (needle_bloom & ~container_bloom) != 0:
 *       needle has bits not in container, so definitely not contained
 */
static inline bool
bloom_may_contain(BloomSig container_bloom, BloomSig needle_bloom)
{
    return (needle_bloom & ~container_bloom) == 0;
}

/*
 * Split 64-bit Bloom signature into two 32-bit keys for GIN storage.
 * GIN uses int32 keys, so we split the signature to get 2 summary keys.
 */
static inline void
bloom_split_for_gin(BloomSig sig, int32 *lo_key, int32 *hi_key)
{
    *lo_key = (int32)(sig & 0xFFFFFFFF);
    *hi_key = (int32)(sig >> 32);
}

/*
 * Reconstruct 64-bit Bloom signature from two 32-bit GIN keys.
 */
static inline BloomSig
bloom_from_gin_keys(int32 lo_key, int32 hi_key)
{
    return ((BloomSig)(uint32)hi_key << 32) | (BloomSig)(uint32)lo_key;
}

/* Function declarations - Parser */
extern Sexp *sexp_parse(const char *input, int len);
extern void sexp_parse_value(SexpParseState *state);

/* Function declarations - I/O */
extern char *sexp_to_cstring(Sexp *sexp);
extern void sexp_to_string_internal(Sexp *sexp, char *data, int len, StringInfo buf);

/* Function declarations - Operations */
extern bool sexp_equal(Sexp *a, Sexp *b);
extern Sexp *sexp_car(Sexp *sexp);
extern Sexp *sexp_cdr(Sexp *sexp);
extern int32 sexp_length(Sexp *sexp);
extern Sexp *sexp_nth(Sexp *sexp, int32 n);
extern bool sexp_contains(Sexp *container, Sexp *element);
extern bool sexp_contains_key(Sexp *container, Sexp *needle);
extern Sexp *sexp_head(Sexp *sexp);
extern uint32 sexp_compute_hash(Sexp *sexp);
extern uint32 sexp_element_hash(uint8 *ptr, uint8 *end, char **symbols, int *sym_lengths, int sym_count);

/* Packed varlena variants for read-only operations (avoid detoast copy) */
extern bool sexp_equal_packed(struct varlena *a, struct varlena *b);
extern bool sexp_contains_packed(struct varlena *container, struct varlena *element);
extern uint32 sexp_compute_hash_packed(struct varlena *packed);

/* Function declarations - Bloom signature */
extern BloomSig sexp_compute_bloom(Sexp *sexp);
extern BloomSig sexp_element_bloom(uint8 *ptr, uint8 *end, char **symbols, int *sym_lengths, int sym_count);

/*
 * Pattern Matching
 * ================
 * 
 * Pattern syntax:
 *   _       - Match any single element (wildcard)
 *   _*      - Match zero or more elements (rest/spread)
 *   ?name   - Capture single element as 'name'
 *   ??name  - Capture rest of elements as 'name'
 *   literal - Match exactly
 *
 * Examples:
 *   (define _ _)        - matches any define with 2 arguments
 *   (+ _*)              - matches + with any number of arguments  
 *   (define ?name ?val) - captures name and value
 *   (list ??items)      - captures all items after 'list'
 */

/* Pattern element types */
typedef enum PatternType
{
    PAT_LITERAL,        /* Exact match required */
    PAT_WILDCARD,       /* _ - match any single element */
    PAT_WILDCARD_REST,  /* _* - match zero or more elements */
    PAT_CAPTURE,        /* ?name - capture single element */
    PAT_CAPTURE_REST    /* ??name - capture rest of elements */
} PatternType;

/* Maximum captures in a pattern */
#define SEXP_MAX_CAPTURES 32

/* Capture storage */
typedef struct SexpCapture
{
    char        name[64];       /* Capture name (from ?name) */
    Sexp       *value;          /* Captured value */
    bool        is_rest;        /* True if this is a ??rest capture */
    int         rest_count;     /* Number of elements in rest capture */
    Sexp      **rest_values;    /* Array of captured values for rest */
} SexpCapture;

typedef struct SexpMatchResult
{
    bool        matched;        /* Did the pattern match? */
    int         capture_count;  /* Number of captures */
    SexpCapture captures[SEXP_MAX_CAPTURES];
} SexpMatchResult;

/* Function declarations - Pattern matching */
extern bool sexp_match(Sexp *expr, Sexp *pattern);
extern bool sexp_match_with_captures(Sexp *expr, Sexp *pattern, SexpMatchResult *result);
extern Sexp *sexp_find_first(Sexp *expr, Sexp *pattern);

/* Function declarations - Read state management */
extern void sexp_init_read_state(SexpReadState *state, Sexp *sexp);
extern void sexp_init_read_state_packed(SexpReadState *state, struct varlena *packed);
extern void sexp_free_read_state(SexpReadState *state);
extern SexpType sexp_read_type(SexpReadState *state);
extern SexpType sexp_get_type(Sexp *sexp);
extern SexpType sexp_get_type_packed(struct varlena *packed);

/* Function declarations - Element navigation */
extern uint8 *sexp_skip_element(uint8 *ptr, uint8 *end);

/*
 * Singleton NIL allocation
 * ========================
 * Returns a cached singleton NIL sexp allocated in TopMemoryContext.
 * This avoids repeated palloc for the common case of returning empty lists.
 */
extern Sexp *sexp_get_nil_singleton(void);

/*
 * Internal variants for nested operations
 * =======================================
 * These accept a caller-managed read state to avoid repeated init/free
 * during deep recursion. Used for containment, equality, and comparison.
 */
extern bool sexp_equal_internal(SexpReadState *a_state, SexpReadState *b_state);
extern bool sexp_contains_internal(SexpReadState *container_state, SexpReadState *elem_state);

/* Convenience macro */
#define SEXP_TYPE(s) sexp_get_type(s)
#define SEXP_TYPE_PACKED(p) sexp_get_type_packed(p)

/*
 * Utility macros for argument fetching
 * ====================================
 * 
 * PG_GETARG_SEXP: Standard detoast - use when you need a writable copy
 *                 or will return/modify the sexp.
 *
 * PG_GETARG_SEXP_PACKED: Packed detoast - use for READ-ONLY operations.
 *                        Returns potentially-short varlena without copying.
 *                        MUST use VARDATA_ANY/VARSIZE_ANY_EXHDR macros.
 *
 * PG_GETARG_SEXP_COPY: Always copy - use when you need to modify and keep.
 */
#define DatumGetSexp(d)         ((Sexp *) PG_DETOAST_DATUM(d))
#define DatumGetSexpCopy(d)     ((Sexp *) PG_DETOAST_DATUM_COPY(d))
#define DatumGetSexpPacked(d)   ((struct varlena *) PG_DETOAST_DATUM_PACKED(d))
#define PG_GETARG_SEXP(n)       DatumGetSexp(PG_GETARG_DATUM(n))
#define PG_GETARG_SEXP_COPY(n)  DatumGetSexpCopy(PG_GETARG_DATUM(n))
#define PG_GETARG_SEXP_PACKED(n) DatumGetSexpPacked(PG_GETARG_DATUM(n))
#define PG_RETURN_SEXP(x)       PG_RETURN_POINTER(x)

/*
 * Macros for accessing packed sexp data
 * Must be used with PG_GETARG_SEXP_PACKED results
 */
#define SEXP_PACKED_DATA_PTR(p)  ((uint8 *)VARDATA_ANY(p) + 1)  /* Skip version byte */
#define SEXP_PACKED_DATA_SIZE(p) (VARSIZE_ANY_EXHDR(p) - 1)     /* Exclude version byte */
#define SEXP_PACKED_VERSION(p)   (*((uint8 *)VARDATA_ANY(p)))

#endif /* PG_SEXP_H */
