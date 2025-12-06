/*
 * sexp_ops.c
 *
 * S-expression operations - car, cdr, nth, contains, etc.
 *
 * Performance notes:
 * - Element extraction reuses parent's symbol table header (zero-copy when possible)
 * - Containment checking compares elements in-place without allocations
 * - Skip operations are O(1) for atoms, O(n) only for large lists without size prefix
 */

#include "pg_sexp.h"
#include "sexp_debug.h"
#include <string.h>

/* Forward declarations */
static void skip_element(SexpReadState *state);
static Sexp *extract_element_fast(Sexp *parent, uint8 *elem_start, uint8 *elem_end);
static Sexp *build_nil_sexp(void);
static void write_varint_to_buf(StringInfo buf, uint64 value);
static bool elements_equal_recursive(SexpReadState *state_a, SexpReadState *state_b);

/*
 * Helper structure for decoded list headers (v6 format)
 * Provides a consistent interface for both small and large lists.
 */
typedef struct ListHeader
{
    uint64   count;           /* Number of elements */
    uint32   hash;            /* Structural hash (0 for small lists) */
    SEntry  *sentries;        /* SEntry table (NULL for small lists) */
    uint8   *data_start;      /* Start of element data */
    bool     is_large;        /* True if large list format */
} ListHeader;

/*
 * Decode a list header after the tag byte has been read.
 * 
 * V6 FORMAT:
 * - Small list (count in tag): [tag|count][varint:size][elements...]
 * - Large list (count=0):      [tag][uint32:count][uint32:hash][SEntry table][elements...]
 * 
 * Input: tag_byte is the first byte (already consumed), ptr points to data after tag.
 * Output: Fills hdr structure, advances state->ptr past header to data start.
 */
static inline void
decode_list_header(SexpReadState *state, uint8 tag_byte, ListHeader *hdr)
{
    uint64 count = tag_byte & SEXP_DATA_MASK;
    
    if (unlikely(count == 0))
    {
        /* Large list with SEntry table - less common for typical workloads */
        uint32 cnt32;
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state->ptr);
        state->ptr += sizeof(uint32);
        hdr->count = cnt32;
        
        hdr->hash = SEXP_LOAD_UINT32_UNALIGNED(state->ptr);
        state->ptr += sizeof(uint32);
        
        hdr->sentries = (SEntry *)state->ptr;
        state->ptr += hdr->count * sizeof(SEntry);
        hdr->data_start = state->ptr;
        hdr->is_large = true;
    }
    else
    {
        /* Small list v6: skip size prefix - common case */
        (void)decode_varint(&state->ptr, state->end);
        hdr->count = count;
        hdr->hash = 0;
        hdr->sentries = NULL;
        hdr->data_start = state->ptr;
        hdr->is_large = false;
    }
}

/*
 * OPTIMIZATION: Fast car access for small lists.
 * 
 * For head-only access (car) or small nth, skip reading list-size varint.
 * Just scan forward to the element; only read size when explicit bounds needed.
 * 
 * This saves the varint decode overhead which, while small, adds up for
 * frequent car operations on small lists.
 * 
 * Returns pointer to first element, or NULL if list is empty.
 * For large lists, falls back to full header decode.
 */
static inline uint8 *
decode_list_car_fast(SexpReadState *state, uint8 tag_byte, uint64 *out_count)
{
    uint64 count = tag_byte & SEXP_DATA_MASK;
    
    if (unlikely(count == 0))
    {
        /* Large list - must read full header for SEntry access */
        uint32 cnt32;
        SEntry *sentries;
        uint8 *data_start;
        
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state->ptr);
        state->ptr += sizeof(uint32);
        *out_count = cnt32;
        
        /* Skip hash */
        state->ptr += sizeof(uint32);
        
        if (unlikely(cnt32 == 0))
            return NULL;
        
        /* Get first element via SEntry[0] */
        sentries = (SEntry *)state->ptr;
        data_start = state->ptr + cnt32 * sizeof(SEntry);
        return data_start + SENTRY_GET_OFFSET(sentries[0]);
    }
    else
    {
        /* Small list v6: SKIP size varint, just advance past it */
        *out_count = count;
        
        /* Skip the size varint without fully decoding it */
        while (state->ptr < state->end && (*state->ptr & 0x80))
            state->ptr++;
        if (likely(state->ptr < state->end))
            state->ptr++;  /* Skip final byte of varint */
        
        /* First element is right here */
        return state->ptr;
    }
}

/*
 * Get bounds of element N in a list.
 * For large lists: O(1) via SEntry table
 * For small lists: O(N) scan (but small lists have at most 4 elements now)
 */
static inline void
get_element_bounds(ListHeader *hdr, int idx, uint8 *end, uint8 **elem_start, uint8 **elem_end)
{
    if (hdr->sentries)
    {
        /* Large list: O(1) access via SEntry */
        *elem_start = hdr->data_start + SENTRY_GET_OFFSET(hdr->sentries[idx]);
        *elem_end = (idx + 1 < (int)hdr->count) ?
            (hdr->data_start + SENTRY_GET_OFFSET(hdr->sentries[idx + 1])) : end;
    }
    else
    {
        /* Small list: scan to element (at most 4 elements) */
        SexpReadState temp;
        int j;
        temp.ptr = hdr->data_start;
        temp.end = end;
        temp.data = hdr->data_start;
        temp.symbols = NULL;
        temp.sym_lengths = NULL;
        temp.sym_hashes = NULL;
        temp.sym_count = 0;
        
        for (j = 0; j < idx; j++)
            skip_element(&temp);
        
        *elem_start = temp.ptr;
        skip_element(&temp);
        *elem_end = temp.ptr;
    }
}

/*
 * sexp_equal - Check equality of two s-expressions
 * 
 * IMPORTANT: Cannot use binary comparison because symbol tables may differ
 * between two semantically equal sexps. For example:
 *   car('(a b c)') has symbol table ["a", "b", "c"]
 *   'a' parsed fresh has symbol table ["a"]
 * Both represent the symbol 'a' but have different binary representations.
 * 
 * We must use semantic comparison via elements_equal_recursive which
 * compares actual symbol strings, not indices.
 */
bool
sexp_equal(Sexp *a, Sexp *b)
{
    SexpReadState state_a, state_b;
    bool result;
    
    /* Quick check: if binary equal, definitely equal */
    int size_a = VARSIZE(a) - VARHDRSZ;
    int size_b = VARSIZE(b) - VARHDRSZ;
    if (size_a == size_b && memcmp(&a->version, &b->version, size_a) == 0)
        return true;
    
    /* Do semantic comparison */
    sexp_init_read_state(&state_a, a);
    sexp_init_read_state(&state_b, b);
    
    result = elements_equal_recursive(&state_a, &state_b);
    
    sexp_free_read_state(&state_a);
    sexp_free_read_state(&state_b);
    
    return result;
}

/*
 * sexp_equal_packed - Equality check for packed varlena
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 * Use this variant when both arguments come from PG_GETARG_SEXP_PACKED.
 */
bool
sexp_equal_packed(struct varlena *a, struct varlena *b)
{
    SexpReadState state_a, state_b;
    bool result;
    
    /* Quick check: if binary equal, definitely equal */
    int size_a = VARSIZE_ANY_EXHDR(a);
    int size_b = VARSIZE_ANY_EXHDR(b);
    if (size_a == size_b && memcmp(VARDATA_ANY(a), VARDATA_ANY(b), size_a) == 0)
        return true;
    
    /* Do semantic comparison */
    sexp_init_read_state_packed(&state_a, a);
    sexp_init_read_state_packed(&state_b, b);
    
    result = elements_equal_recursive(&state_a, &state_b);
    
    sexp_free_read_state(&state_a);
    sexp_free_read_state(&state_b);
    
    return result;
}

/*
 * Helper: Create a nil sexp
 * 
 * OPTIMIZED: Uses the singleton NIL sexp allocated in TopMemoryContext
 * to avoid repeated palloc for this common case.
 */
static Sexp *
build_nil_sexp(void)
{
    return sexp_get_nil_singleton();
}

/*
 * Helper: Skip over current element in read state
 * 
 * V6 OPTIMIZATION: Small lists now have a size prefix, enabling O(1) skip
 * without iterating through all elements. This is critical for containment
 * performance on nested structures.
 */
static void
skip_element(SexpReadState *state)
{
    uint8 byte;
    uint8 tag;
    uint64 count;
    uint64 len;
    
    if (unlikely(state->ptr >= state->end))
        return;
    
    byte = *state->ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            /* Nothing more to skip */
            break;
            
        case SEXP_TAG_SMALLINT:
            /* Value is inline, nothing more */
            break;
            
        case SEXP_TAG_INTEGER:
            /* Skip varint */
            (void)decode_varint(&state->ptr, state->end);
            break;
            
        case SEXP_TAG_FLOAT:
            state->ptr += sizeof(float8);
            break;
            
        case SEXP_TAG_SYMBOL_REF:
            /* Skip varint index */
            (void)decode_varint(&state->ptr, state->end);
            break;
            
        case SEXP_TAG_SHORT_STRING:
            len = byte & SEXP_DATA_MASK;
            state->ptr += len;
            break;
            
        case SEXP_TAG_LONG_STRING:
            len = decode_varint(&state->ptr, state->end);
            state->ptr += len;
            break;
            
        case SEXP_TAG_LIST:
        {
            /* Check if small list (count in tag) or large list with SEntry table */
            count = byte & SEXP_DATA_MASK;
            if (unlikely(count == 0))
            {
                /* Large list with SEntry table (v6 format: count + hash + sentries + data) */
                uint32 cnt32;
                cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state->ptr);
                state->ptr += sizeof(uint32);
                count = cnt32;
                
                /* Skip structural hash */
                state->ptr += sizeof(uint32);
                
                /* 
                 * Calculate total skip size: SEntry table + all element data
                 * The last SEntry's offset + that element's size gives total data size,
                 * but we don't store that directly. Instead we compute:
                 * total = sizeof(SEntry)*count + (end_offset - 0)
                 * Since we don't have end_offset stored, we need to scan or use parent bounds.
                 * 
                 * OPTIMIZATION: For large lists, the SEntry table already tells us offsets,
                 * and we can compute total size from the parent container's bounds.
                 * For now, use the simpler approach: skip SEntry table, then skip each element.
                 * 
                 * TODO: Consider storing total_payload_size for large lists too.
                 */
                state->ptr += count * sizeof(SEntry);
                
                /* Skip all elements (required for now, but bounded by SEntry data) */
                {
                    uint64 i;
                    for (i = 0; i < count; i++)
                        skip_element(state);
                }
            }
            else
            {
                /* 
                 * Small list v6 format: [tag|count][varint:size][elements...]
                 * O(1) skip using the size prefix!
                 */
                uint64 payload_size = decode_varint(&state->ptr, state->end);
                state->ptr += payload_size;
            }
            break;
        }
    }
}

/*
 * Public interface: Skip over an element and return pointer to next element.
 * Used by GIN indexing code.
 */
uint8 *
sexp_skip_element(uint8 *ptr, uint8 *end)
{
    SexpReadState temp;
    temp.ptr = ptr;
    temp.end = end;
    temp.data = ptr;
    temp.symbols = NULL;
    temp.sym_lengths = NULL;
    temp.sym_hashes = NULL;
    temp.sym_count = 0;
    
    skip_element(&temp);
    return temp.ptr;
}

/*
 * FAST PATH: Extract element by reusing parent's header.
 * 
 * Instead of rebuilding the symbol table, we copy the parent's entire
 * header (version + symbol table) and just append the element data.
 * This is O(header_size + element_size) instead of O(symbol_table_rebuild).
 */
static Sexp *
extract_element_fast(Sexp *parent, uint8 *elem_start, uint8 *elem_end)
{
    Sexp *result;
    int header_size;
    int elem_size;
    int total_size;
    uint8 *parent_data;
    uint8 *ptr;
    uint64 sym_count;
    int i;
    
    parent_data = (uint8 *)&parent->version;
    ptr = parent_data + 1;  /* Skip version byte */
    
    /* Decode symbol count to find end of symbol table */
    sym_count = decode_varint(&ptr, parent_data + VARSIZE(parent) - VARHDRSZ);
    for (i = 0; i < (int)sym_count; i++)
    {
        uint64 slen = decode_varint(&ptr, parent_data + VARSIZE(parent) - VARHDRSZ);
        ptr += slen;
    }
    
    /* header_size = version + symbol table */
    header_size = (int)(ptr - parent_data);
    elem_size = (int)(elem_end - elem_start);
    
    /* Allocate result: varlena header + our header + element */
    total_size = VARHDRSZ + header_size + elem_size;
    result = (Sexp *) palloc(total_size);
    SET_VARSIZE(result, total_size);
    
    /* Copy header from parent */
    memcpy(&result->version, parent_data, header_size);
    
    /* Copy element data */
    memcpy(((uint8 *)&result->version) + header_size, elem_start, elem_size);
    
    return result;
}

/*
 * Compare two elements for equality without building Sexp objects.
 * Both states must be positioned at the start of elements to compare.
 * Returns true if equal, advances both states past the elements.
 */
static bool
elements_equal_recursive(SexpReadState *state_a, SexpReadState *state_b)
{
    uint8 byte_a, byte_b;
    uint8 tag_a, tag_b;
    
    if (unlikely(state_a->ptr >= state_a->end || state_b->ptr >= state_b->end))
        return (state_a->ptr >= state_a->end) && (state_b->ptr >= state_b->end);
    
    byte_a = *state_a->ptr++;
    byte_b = *state_b->ptr++;
    tag_a = byte_a & SEXP_TAG_MASK;
    tag_b = byte_b & SEXP_TAG_MASK;
    
    /* Different tags = not equal (with exception for symbol comparison) */
    if (tag_a != tag_b)
        return false;
    
    switch (tag_a)
    {
        case SEXP_TAG_NIL:
            return true;
            
        case SEXP_TAG_SMALLINT:
            /* Value is in the byte itself */
            return byte_a == byte_b;
            
        case SEXP_TAG_INTEGER:
        {
            uint64 val_a = decode_varint(&state_a->ptr, state_a->end);
            uint64 val_b = decode_varint(&state_b->ptr, state_b->end);
            return val_a == val_b;
        }
        
        case SEXP_TAG_FLOAT:
        {
            float8 val_a, val_b;
            memcpy(&val_a, state_a->ptr, sizeof(float8));
            memcpy(&val_b, state_b->ptr, sizeof(float8));
            state_a->ptr += sizeof(float8);
            state_b->ptr += sizeof(float8);
            return val_a == val_b;
        }
        
        case SEXP_TAG_SYMBOL_REF:
        {
            /* Compare symbols using pre-computed hashes for speed */
            uint64 idx_a = decode_varint(&state_a->ptr, state_a->end);
            uint64 idx_b = decode_varint(&state_b->ptr, state_b->end);
            
            if ((int)idx_a >= state_a->sym_count || (int)idx_b >= state_b->sym_count)
                return false;
            
            /* Fast path: compare hashes first if available */
            if (state_a->sym_hashes != NULL && state_b->sym_hashes != NULL)
            {
                if (state_a->sym_hashes[idx_a] != state_b->sym_hashes[idx_b])
                    return false;
            }
            
            /* Compare lengths */
            if (state_a->sym_lengths[idx_a] != state_b->sym_lengths[idx_b])
                return false;
            
            /* Same length (and hash if available) - do full comparison */
            return memcmp(state_a->symbols[idx_a], state_b->symbols[idx_b],
                         state_a->sym_lengths[idx_a]) == 0;
        }
        
        case SEXP_TAG_SHORT_STRING:
        {
            int len_a = byte_a & SEXP_DATA_MASK;
            int len_b = byte_b & SEXP_DATA_MASK;
            bool result;
            
            if (len_a != len_b)
            {
                state_a->ptr += len_a;
                state_b->ptr += len_b;
                return false;
            }
            
            result = (memcmp(state_a->ptr, state_b->ptr, len_a) == 0);
            state_a->ptr += len_a;
            state_b->ptr += len_b;
            return result;
        }
        
        case SEXP_TAG_LONG_STRING:
        {
            uint64 len_a = decode_varint(&state_a->ptr, state_a->end);
            uint64 len_b = decode_varint(&state_b->ptr, state_b->end);
            bool result;
            
            if (len_a != len_b)
            {
                state_a->ptr += len_a;
                state_b->ptr += len_b;
                return false;
            }
            
            result = (memcmp(state_a->ptr, state_b->ptr, len_a) == 0);
            state_a->ptr += len_a;
            state_b->ptr += len_b;
            return result;
        }
        
        case SEXP_TAG_LIST:
        {
            uint64 count_a, count_b;
            uint64 i;
            
            /* Decode list counts with v6 format */
            count_a = byte_a & SEXP_DATA_MASK;
            if (unlikely(count_a == 0))
            {
                /* Large list with SEntry table */
                uint32 cnt32;
                cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state_a->ptr);
                state_a->ptr += sizeof(uint32);
                count_a = cnt32;
                /* Skip structural hash */
                state_a->ptr += sizeof(uint32);
                /* Skip SEntry table */
                state_a->ptr += count_a * sizeof(SEntry);
            }
            else
            {
                /* Small list v6: skip the size prefix */
                (void)decode_varint(&state_a->ptr, state_a->end);
            }
            
            count_b = byte_b & SEXP_DATA_MASK;
            if (unlikely(count_b == 0))
            {
                /* Large list with SEntry table */
                uint32 cnt32;
                cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state_b->ptr);
                state_b->ptr += sizeof(uint32);
                count_b = cnt32;
                /* Skip structural hash */
                state_b->ptr += sizeof(uint32);
                /* Skip SEntry table */
                state_b->ptr += count_b * sizeof(SEntry);
            }
            else
            {
                /* Small list v6: skip the size prefix */
                (void)decode_varint(&state_b->ptr, state_b->end);
            }
            
            if (count_a != count_b)
            {
                /* Skip remaining elements */
                for (i = 0; i < count_a; i++)
                    skip_element(state_a);
                for (i = 0; i < count_b; i++)
                    skip_element(state_b);
                return false;
            }
            
            for (i = 0; i < count_a; i++)
            {
                if (!elements_equal_recursive(state_a, state_b))
                {
                    /* Skip remaining elements */
                    uint64 j;
                    for (j = i + 1; j < count_a; j++)
                    {
                        skip_element(state_a);
                        skip_element(state_b);
                    }
                    return false;
                }
            }
            return true;
        }
        
        default:
            return false;
    }
}

/* Helper: write varint to StringInfo */
static void
write_varint_to_buf(StringInfo buf, uint64 value)
{
    uint8 tmp[10];
    int len = encode_varint(tmp, value);
    appendBinaryStringInfo(buf, (char *)tmp, len);
}

/*
 * sexp_car - Get first element of a list
 * 
 * Uses decode_list_car_fast() to skip size varint for small lists,
 * reducing overhead for this common operation.
 */
Sexp *
sexp_car(Sexp *sexp)
{
    SexpReadState state;
    uint8 byte;
    uint8 tag;
    uint8 *elem_start;
    uint64 count;
    Sexp *result;
    SexpReadState temp;
    
    sexp_init_read_state(&state, sexp);
    
    if (unlikely(state.ptr >= state.end))
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    byte = *state.ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    if (unlikely(tag == SEXP_TAG_NIL))
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    if (unlikely(tag != SEXP_TAG_LIST))
    {
        sexp_free_read_state(&state);
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("car requires a list")));
    }
    
    /* OPTIMIZATION: Use fast car access - skips size varint for small lists */
    elem_start = decode_list_car_fast(&state, byte, &count);
    
    if (count == 0 || elem_start == NULL)
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    /* Skip first element to find its end */
    temp.ptr = elem_start;
    temp.end = state.end;
    temp.data = elem_start;
    temp.symbols = NULL;
    temp.sym_lengths = NULL;
    temp.sym_hashes = NULL;
    temp.sym_count = 0;
    skip_element(&temp);
    
    result = extract_element_fast(sexp, elem_start, temp.ptr);
    
    sexp_free_read_state(&state);
    return result;
}

/*
 * sexp_cdr - Get rest of list (all but first element)
 * Returns a new list containing elements 1..n-1
 * Updated for v6 format with size prefix on small lists
 */
Sexp *
sexp_cdr(Sexp *sexp)
{
    SexpReadState state;
    uint8 byte;
    uint8 tag;
    uint64 i;
    StringInfoData buf;
    StringInfoData elem_buf;
    Sexp *result;
    int total_size;
    int j;
    ListHeader hdr;
    
    sexp_init_read_state(&state, sexp);
    
    if (state.ptr >= state.end)
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    byte = *state.ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    if (tag == SEXP_TAG_NIL)
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    if (tag != SEXP_TAG_LIST)
    {
        sexp_free_read_state(&state);
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("cdr requires a list")));
    }
    
    /* Decode list header */
    decode_list_header(&state, byte, &hdr);
    
    if (hdr.count <= 1)
    {
        sexp_free_read_state(&state);
        return build_nil_sexp();
    }
    
    /* Build new list with count-1 elements */
    initStringInfo(&buf);
    initStringInfo(&elem_buf);
    
    /* Write version */
    appendStringInfoChar(&buf, SEXP_FORMAT_VERSION);
    
    /* Write symbol table */
    write_varint_to_buf(&buf, state.sym_count);
    for (j = 0; j < state.sym_count; j++)
    {
        write_varint_to_buf(&buf, state.sym_lengths[j]);
        appendBinaryStringInfo(&buf, state.symbols[j], state.sym_lengths[j]);
    }
    
    /* Collect elements 1..count-1 */
    for (i = 1; i < hdr.count; i++)
    {
        uint8 *elem_start, *elem_end;
        int elem_size;
        
        get_element_bounds(&hdr, (int)i, state.end, &elem_start, &elem_end);
        elem_size = (int)(elem_end - elem_start);
        appendBinaryStringInfo(&elem_buf, (char *)elem_start, elem_size);
    }
    
    /* 
     * Write new list in v6 format.
     * For small lists (count-1 <= 4), use compact format with size prefix.
     * For large lists, include SEntry table.
     */
    if (hdr.count - 1 <= SEXP_SMALL_LIST_MAX)
    {
        /* Small list v6: tag with inline count + size prefix */
        appendStringInfoChar(&buf, SEXP_TAG_LIST | (uint8)(hdr.count - 1));
        write_varint_to_buf(&buf, (uint64)elem_buf.len);
        appendBinaryStringInfo(&buf, elem_buf.data, elem_buf.len);
    }
    else
    {
        /* Large list: need to build SEntry table */
        SEntry *new_sentries;
        uint32 new_count = (uint32)(hdr.count - 1);
        uint32 dummy_hash = 0;  /* TODO: compute proper hash */
        
        new_sentries = palloc(sizeof(SEntry) * new_count);
        
        /* Build SEntry table by re-scanning elements */
        {
            uint8 *scan_ptr = (uint8 *)elem_buf.data;
            uint8 *scan_end = scan_ptr + elem_buf.len;
            
            for (i = 0; i < new_count; i++)
            {
                uint8 first_byte = *scan_ptr;
                uint32 stype;
                int offset = (int)(scan_ptr - (uint8 *)elem_buf.data);
                SexpReadState temp;
                
                /* Get type from first byte */
                switch (first_byte & SEXP_TAG_MASK)
                {
                    case SEXP_TAG_NIL: stype = SENTRY_TYPE_NIL; break;
                    case SEXP_TAG_SMALLINT:
                    case SEXP_TAG_INTEGER: stype = SENTRY_TYPE_INTEGER; break;
                    case SEXP_TAG_FLOAT: stype = SENTRY_TYPE_FLOAT; break;
                    case SEXP_TAG_SYMBOL_REF: stype = SENTRY_TYPE_SYMBOL; break;
                    case SEXP_TAG_SHORT_STRING:
                    case SEXP_TAG_LONG_STRING: stype = SENTRY_TYPE_STRING; break;
                    case SEXP_TAG_LIST: stype = SENTRY_TYPE_LIST; break;
                    default: stype = SENTRY_TYPE_NIL; break;
                }
                new_sentries[i] = SENTRY_MAKE(stype, offset);
                
                /* Skip to next element */
                temp.ptr = scan_ptr;
                temp.end = scan_end;
                temp.data = scan_ptr;
                temp.symbols = NULL;
                temp.sym_lengths = NULL;
                temp.sym_hashes = NULL;
                temp.sym_count = 0;
                skip_element(&temp);
                scan_ptr = temp.ptr;
            }
        }
        
        /* Write list header */
        appendStringInfoChar(&buf, SEXP_TAG_LIST);  /* Tag with 0 in data bits */
        appendBinaryStringInfo(&buf, (char *)&new_count, sizeof(uint32));
        appendBinaryStringInfo(&buf, (char *)&dummy_hash, sizeof(uint32));
        
        /* Write SEntry table */
        appendBinaryStringInfo(&buf, (char *)new_sentries, new_count * sizeof(SEntry));
        
        /* Write elements */
        appendBinaryStringInfo(&buf, elem_buf.data, elem_buf.len);
        
        pfree(new_sentries);
    }
    
    pfree(elem_buf.data);
    
    /* Create result */
    total_size = VARHDRSZ + buf.len;
    result = (Sexp *) palloc(total_size);
    SET_VARSIZE(result, total_size);
    memcpy(&result->version, buf.data, buf.len);
    
    pfree(buf.data);
    sexp_free_read_state(&state);
    
    return result;
}

/*
 * sexp_length - Get number of elements in a list
 * O(1) operation - count is stored in header
 */
int32
sexp_length(Sexp *sexp)
{
    SexpReadState state;
    uint8 byte;
    uint8 tag;
    uint64 count;
    
    sexp_init_read_state(&state, sexp);
    
    if (state.ptr >= state.end)
    {
        sexp_free_read_state(&state);
        return 0;
    }
    
    byte = *state.ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    if (tag == SEXP_TAG_NIL)
    {
        sexp_free_read_state(&state);
        return 0;
    }
    
    if (tag != SEXP_TAG_LIST)
    {
        sexp_free_read_state(&state);
        return 1;  /* Atoms have length 1 */
    }
    
    /* Decode list count - already consumed tag byte above */
    count = byte & SEXP_DATA_MASK;
    if (unlikely(count == 0))
    {
        /* Large list with SEntry table (v6): read uint32 count */
        uint32 cnt32;
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state.ptr);
        count = cnt32;
        /* Note: no need to skip hash here, we just need count */
    }
    
    sexp_free_read_state(&state);
    return (int32)count;
}

/*
 * sexp_nth - Get nth element of a list (0-indexed)
 * OPTIMIZED: Uses SEntry table for O(1) access, or linear scan for small lists
 */
Sexp *
sexp_nth(Sexp *sexp, int32 n)
{
    SexpReadState state;
    uint8 byte;
    uint8 tag;
    uint8 *elem_start;
    uint8 *elem_end;
    ListHeader hdr;
    Sexp *result;
    
    if (n < 0)
        return NULL;
    
    sexp_init_read_state(&state, sexp);
    
    if (state.ptr >= state.end)
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    byte = *state.ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    if (tag == SEXP_TAG_NIL)
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    if (tag != SEXP_TAG_LIST)
    {
        if (n == 0)
        {
            /* Return the atom itself */
            sexp_free_read_state(&state);
            return sexp;
        }
        sexp_free_read_state(&state);
        return NULL;
    }
    
    /* Decode list header */
    decode_list_header(&state, byte, &hdr);
    
    if ((uint64)n >= hdr.count)
    {
        sexp_free_read_state(&state);
        return NULL;
    }
    
    /* Get element bounds - O(1) for large lists, O(n) for small (max 4 elements) */
    get_element_bounds(&hdr, n, state.end, &elem_start, &elem_end);
    
    result = extract_element_fast(sexp, elem_start, elem_end);
    
    sexp_free_read_state(&state);
    return result;
}

/*
 * sexp_head - Get first element (alias for car)
 */
Sexp *
sexp_head(Sexp *sexp)
{
    return sexp_car(sexp);
}

/*
 * Compute semantic hash of an element for hash indexes, joins, and containment checks.
 * This hash MUST mirror equality semantics exactly.
 * 
 * SEMANTIC HASH RULES:
 * - Two semantically equal values MUST have the same hash
 * - Different types SHOULD have different hashes (include type tags)
 * - Symbols: hash by symbol TEXT, not symbol ID (critical for cross-sexp comparison)
 * - Strings: hash by string content with type tag
 * - Integers: hash canonical int64 value with type tag (smallint == integer)
 * - Floats: hash 64-bit bit pattern with type tag; normalize -0.0 to 0.0
 * - Lists: combine child hashes with position mixing
 * - NIL: returns 0
 * 
 * Uses PostgreSQL's native hash_bytes/hash_combine for mixing.
 */
uint32
sexp_element_hash(uint8 *ptr, uint8 *end, char **symbols, int *sym_lengths, int sym_count)
{
    uint8 byte;
    uint8 tag;
    
    if (ptr >= end)
        return 0;
    
    byte = *ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            return 0;
            
        case SEXP_TAG_SMALLINT:
        {
            /*
             * Smallint and integer are semantically the same type.
             * Hash the canonical int64 value with INTEGER type tag.
             */
            int64 val = (int)(byte & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
            uint32 type_hash = sexp_hash_uint32(SEXP_TAG_INTEGER);
            uint32 value_hash = sexp_hash_int64(val);
            return hash_combine(type_hash, value_hash);
        }
            
        case SEXP_TAG_INTEGER:
        {
            /*
             * Hash canonical zigzag-decoded int64 value with type tag.
             * This matches the smallint case above for same values.
             */
            uint64 encoded = decode_varint(&ptr, end);
            int64 val = zigzag_decode(encoded);
            uint32 type_hash = sexp_hash_uint32(SEXP_TAG_INTEGER);
            uint32 value_hash = sexp_hash_int64(val);
            return hash_combine(type_hash, value_hash);
        }
        
        case SEXP_TAG_FLOAT:
        {
            /*
             * Hash the 64-bit bit pattern with type tag.
             * sexp_hash_float64() normalizes -0.0 to 0.0 to match equality semantics.
             */
            double val;
            uint32 type_hash = sexp_hash_uint32(SEXP_TAG_FLOAT);
            uint32 value_hash;
            memcpy(&val, ptr, sizeof(double));
            value_hash = sexp_hash_float64(val);
            return hash_combine(type_hash, value_hash);
        }
            
        case SEXP_TAG_SYMBOL_REF:
        {
            /*
             * CRITICAL: Hash by symbol TEXT, not symbol ID.
             * Symbol IDs differ between sexps with different symbol tables.
             * Use cached symbol strings from read state for consistent hashing.
             */
            uint64 idx = decode_varint(&ptr, end);
            if ((int)idx < sym_count)
                return sexp_hash_string_with_tag(SEXP_TAG_SYMBOL_REF, 
                                                  symbols[idx], sym_lengths[idx]);
            return 0;
        }
        
        case SEXP_TAG_SHORT_STRING:
        {
            /*
             * Hash string content with STRING type tag.
             * Short and long strings are the same semantic type.
             */
            int len = byte & SEXP_DATA_MASK;
            return sexp_hash_string_with_tag(SEXP_TAG_SHORT_STRING, (char *)ptr, len);
        }
        
        case SEXP_TAG_LONG_STRING:
        {
            /*
             * Hash string content with STRING type tag.
             * Use SEXP_TAG_SHORT_STRING as canonical tag for all strings.
             */
            uint64 len = decode_varint(&ptr, end);
            return sexp_hash_string_with_tag(SEXP_TAG_SHORT_STRING, (char *)ptr, (int)len);
        }
        
        case SEXP_TAG_LIST:
        {
            uint64 count = byte & SEXP_DATA_MASK;
            uint32 list_hash;
            uint64 i;
            
            if (unlikely(count == 0))
            {
                /* Large list - hash is stored! */
                uint32 stored_hash;
                ptr += sizeof(uint32);  /* Skip count */
                stored_hash = SEXP_LOAD_UINT32_UNALIGNED(ptr);
                return stored_hash;
            }
            
            /* Small list v6 - skip size prefix, then compute hash from children */
            {
                SexpReadState temp;
                uint32 child_hash;
                
                /* Skip the size prefix (varint) */
                (void)decode_varint(&ptr, end);
                
                /* Start with count and type hash */
                list_hash = sexp_hash_uint32((uint32)count);
                list_hash = hash_combine(list_hash, sexp_hash_uint32(SEXP_TAG_LIST));
                
                for (i = 0; i < count; i++)
                {
                    child_hash = sexp_element_hash(ptr, end, symbols, sym_lengths, sym_count);
                    list_hash = sexp_hash_combine(list_hash, child_hash, (int)i);
                    
                    /* Skip element */
                    temp.ptr = ptr;
                    temp.end = end;
                    temp.data = ptr;
                    temp.symbols = NULL;
                    temp.sym_lengths = NULL;
                    temp.sym_hashes = NULL;
                    temp.sym_count = 0;
                    skip_element(&temp);
                    ptr = temp.ptr;
                }
                return list_hash;
            }
        }
        
        default:
            return 0;
    }
}

/*
 * Get SEntry type from element tag byte
 */
static inline uint32
get_sentry_type_from_byte(uint8 byte)
{
    uint8 tag = byte & SEXP_TAG_MASK;
    switch (tag)
    {
        case SEXP_TAG_NIL:          return SENTRY_TYPE_NIL;
        case SEXP_TAG_SMALLINT:
        case SEXP_TAG_INTEGER:      return SENTRY_TYPE_INTEGER;
        case SEXP_TAG_FLOAT:        return SENTRY_TYPE_FLOAT;
        case SEXP_TAG_SYMBOL_REF:   return SENTRY_TYPE_SYMBOL;
        case SEXP_TAG_SHORT_STRING:
        case SEXP_TAG_LONG_STRING:  return SENTRY_TYPE_STRING;
        case SEXP_TAG_LIST:         return SENTRY_TYPE_LIST;
        default:                    return SENTRY_TYPE_NIL;
    }
}

/*
 * ===========================================================================
 * Direct Atom Comparison
 * ===========================================================================
 *
 * Type-directed atom comparison without size measurement.
 * For atoms, we decode and compare values directly - no need to measure
 * serialized size first. This eliminates redundant traversals.
 *
 * Returns: true if elements are equal, false otherwise
 * Note: Does NOT advance pointers on inequality (caller handles skipping)
 */
static inline bool
atom_compare_direct(uint8 *a_ptr, uint8 *a_end,
                    char **a_syms, int *a_sym_lens, uint32 *a_sym_hashes, int a_sym_count,
                    uint8 *b_ptr, uint8 *b_end,
                    char **b_syms, int *b_sym_lens, uint32 *b_sym_hashes, int b_sym_count)
{
    uint8 byte_a, byte_b;
    uint8 tag_a, tag_b;
    
    if (unlikely(a_ptr >= a_end || b_ptr >= b_end))
        return false;
    
    byte_a = *a_ptr++;
    byte_b = *b_ptr++;
    tag_a = byte_a & SEXP_TAG_MASK;
    tag_b = byte_b & SEXP_TAG_MASK;
    
    /* Different tags = not equal (except smallint/integer which are same semantic type) */
    if (unlikely(tag_a != tag_b))
    {
        int64 val_a, val_b;
        uint64 encoded;
        
        /* Check for smallint vs integer cross-comparison */
        if (!((tag_a == SEXP_TAG_SMALLINT || tag_a == SEXP_TAG_INTEGER) &&
              (tag_b == SEXP_TAG_SMALLINT || tag_b == SEXP_TAG_INTEGER)))
            return false;
        
        /* Handle smallint vs integer comparison */
        if (tag_a == SEXP_TAG_SMALLINT)
            val_a = (int)(byte_a & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
        else
        {
            encoded = decode_varint(&a_ptr, a_end);
            val_a = zigzag_decode(encoded);
        }
        if (tag_b == SEXP_TAG_SMALLINT)
            val_b = (int)(byte_b & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
        else
        {
            encoded = decode_varint(&b_ptr, b_end);
            val_b = zigzag_decode(encoded);
        }
        return val_a == val_b;
    }
    
    switch (tag_a)
    {
        case SEXP_TAG_NIL:
            return true;
            
        case SEXP_TAG_SMALLINT:
            /* Value is in the byte itself - single comparison */
            return byte_a == byte_b;
            
        case SEXP_TAG_INTEGER:
        {
            /* Decode once and compare - no size measurement needed */
            uint64 val_a = decode_varint(&a_ptr, a_end);
            uint64 val_b = decode_varint(&b_ptr, b_end);
            return val_a == val_b;
        }
        
        case SEXP_TAG_FLOAT:
        {
            /* Direct 8-byte comparison */
            return memcmp(a_ptr, b_ptr, sizeof(float8)) == 0;
        }
        
        case SEXP_TAG_SYMBOL_REF:
        {
            /* Compare symbols: length + hash first, then memcmp on equality */
            uint64 idx_a = decode_varint(&a_ptr, a_end);
            uint64 idx_b = decode_varint(&b_ptr, b_end);
            
            if ((int)idx_a >= a_sym_count || (int)idx_b >= b_sym_count)
                return false;
            
            /* Fast path: compare lengths first (very cheap) */
            if (a_sym_lens[idx_a] != b_sym_lens[idx_b])
                return false;
            
            /* Compare hashes if available (avoids memcmp in most cases) */
            if (a_sym_hashes != NULL && b_sym_hashes != NULL)
            {
                if (a_sym_hashes[idx_a] != b_sym_hashes[idx_b])
                    return false;
            }
            
            /* Same length (and hash) - do full comparison */
            return memcmp(a_syms[idx_a], b_syms[idx_b], a_sym_lens[idx_a]) == 0;
        }
        
        case SEXP_TAG_SHORT_STRING:
        {
            /* Length is inline - compare lengths first, then content */
            int len_a = byte_a & SEXP_DATA_MASK;
            int len_b = byte_b & SEXP_DATA_MASK;
            
            if (len_a != len_b)
                return false;
            
            return memcmp(a_ptr, b_ptr, len_a) == 0;
        }
        
        case SEXP_TAG_LONG_STRING:
        {
            /* Decode lengths, compare, then content */
            uint64 len_a = decode_varint(&a_ptr, a_end);
            uint64 len_b = decode_varint(&b_ptr, b_end);
            
            if (len_a != len_b)
                return false;
            
            return memcmp(a_ptr, b_ptr, len_a) == 0;
        }
        
        default:
            /* Lists and unknown tags - fall back to full comparison */
            return false;
    }
}

/*
 * Fast containment check with multiple optimization strategies:
 * 1. Type filtering: skip elements of wrong type using SEntry types
 * 2. Hash-based rejection: use structural hash for quick rejection on lists
 * 3. First-byte check: quick rejection without full comparison
 * 4. Direct comparison: compare values inline without measuring size first
 */
static bool
contains_fast_scan(uint8 *container_ptr, uint8 *container_end,
                   char **container_syms, int *container_sym_lens, 
                   uint32 *container_sym_hashes, int container_sym_count,
                   uint8 *elem_ptr, uint8 *elem_end, uint32 elem_hash,
                   char **elem_syms, int *elem_sym_lens, 
                   uint32 *elem_sym_hashes, int elem_sym_count,
                   uint8 elem_first_byte, uint32 elem_stype)
{
    uint8 byte;
    uint8 tag;
    uint32 container_stype;
    
    if (unlikely(container_ptr >= container_end))
        return false;
    
    byte = *container_ptr;
    tag = byte & SEXP_TAG_MASK;
    container_stype = get_sentry_type_from_byte(byte);
    
    /* 
     * Type-aware matching: 
     * - If element is not a list, only check if container has same type
     * - This is a very fast first-level filter
     */
    if (likely(container_stype == elem_stype))
    {
        /* First byte check - if bytes match, likely a match candidate */
        if (byte == elem_first_byte)
        {
            /*
             * OPTIMIZATION: Use atom_compare_direct() for non-list elements.
             * This avoids building SexpReadState and calling the recursive
             * comparison function. For atoms, direct comparison is much faster.
             */
            if (elem_stype != SENTRY_TYPE_LIST)
            {
                /* Direct atom comparison - no SexpReadState overhead */
                if (atom_compare_direct(container_ptr, container_end,
                                        container_syms, container_sym_lens,
                                        container_sym_hashes, container_sym_count,
                                        elem_ptr, elem_end,
                                        elem_syms, elem_sym_lens,
                                        elem_sym_hashes, elem_sym_count))
                    return true;
            }
            else
            {
                /* Lists require full recursive comparison */
                SexpReadState cstate, estate;
                cstate.ptr = container_ptr;
                cstate.end = container_end;
                cstate.data = container_ptr;
                cstate.symbols = container_syms;
                cstate.sym_lengths = container_sym_lens;
                cstate.sym_count = container_sym_count;
                cstate.sym_hashes = container_sym_hashes;
                
                estate.ptr = elem_ptr;
                estate.end = elem_end;
                estate.data = elem_ptr;
                estate.symbols = elem_syms;
                estate.sym_lengths = elem_sym_lens;
                estate.sym_count = elem_sym_count;
                estate.sym_hashes = elem_sym_hashes;
                
                if (elements_equal_recursive(&cstate, &estate))
                    return true;
            }
        }
    }
    
    /* If container element is a list, recurse into children */
    if (tag == SEXP_TAG_LIST)
    {
        uint64 count;
        uint64 i;
        uint8 *ptr = container_ptr + 1;
        
        count = byte & SEXP_DATA_MASK;
        if (unlikely(count == 0))
        {
            /* Large list with SEntry table */
            uint32 cnt32;
            SEntry *sentries;
            uint8 *data_start;
            
            cnt32 = SEXP_LOAD_UINT32_UNALIGNED(ptr);
            ptr += sizeof(uint32);
            count = cnt32;
            
            /* Skip structural hash */
            ptr += sizeof(uint32);
            
            sentries = (SEntry *)ptr;
            data_start = ptr + count * sizeof(SEntry);
            
            /*
             * OPTIMIZATION: Prefetch first few child data locations.
             * This hides memory latency by starting fetches early.
             */
            #define PREFETCH_AHEAD 4
            {
                uint64 p;
                for (p = 0; p < count && p < PREFETCH_AHEAD; p++)
                {
                    uint8 *prefetch_ptr = data_start + SENTRY_GET_OFFSET(sentries[p]);
                    __builtin_prefetch(prefetch_ptr, 0, 1);  /* read, low locality */
                }
            }
            
            for (i = 0; i < count; i++)
            {
                uint8 *child_ptr = data_start + SENTRY_GET_OFFSET(sentries[i]);
                uint8 *child_end = (i + 1 < count) ? 
                    (data_start + SENTRY_GET_OFFSET(sentries[i + 1])) : container_end;
                uint32 child_stype = SENTRY_GET_TYPE(sentries[i]);
                
                /*
                 * OPTIMIZATION: Prefetch ahead while processing current child.
                 * Prefetch child data PREFETCH_AHEAD iterations ahead.
                 */
                if (i + PREFETCH_AHEAD < count)
                {
                    uint8 *prefetch_ptr = data_start + SENTRY_GET_OFFSET(sentries[i + PREFETCH_AHEAD]);
                    __builtin_prefetch(prefetch_ptr, 0, 1);
                }
                
                /*
                 * TYPE FILTERING: Only recurse if child could contain element.
                 * - If element is not a list, child must have same type OR be a list
                 * - If element is a list, child must be a list
                 */
                if (child_stype == elem_stype || child_stype == SENTRY_TYPE_LIST)
                {
                    if (contains_fast_scan(child_ptr, child_end,
                                           container_syms, container_sym_lens,
                                           container_sym_hashes, container_sym_count,
                                           elem_ptr, elem_end, elem_hash,
                                           elem_syms, elem_sym_lens,
                                           elem_sym_hashes, elem_sym_count,
                                           elem_first_byte, elem_stype))
                        return true;
                }
            }
            #undef PREFETCH_AHEAD
        }
        else
        {
            /* Small list v6 - skip size prefix, then scan sequentially */
            uint64 payload_size;
            uint8 *data_end;
            SexpReadState temp;
            
            /* Read the size prefix - gives us exact bounds for this list's data */
            payload_size = decode_varint(&ptr, container_end);
            data_end = ptr + payload_size;
            if (data_end > container_end)
                data_end = container_end;
            
            temp.ptr = ptr;
            temp.end = data_end;
            temp.data = ptr;
            temp.symbols = NULL;
            temp.sym_lengths = NULL;
            temp.sym_hashes = NULL;
            temp.sym_count = 0;
            
            for (i = 0; i < count && temp.ptr < data_end; i++)
            {
                uint8 *child_start = temp.ptr;
                uint8 child_byte = *child_start;
                uint32 child_stype = get_sentry_type_from_byte(child_byte);
                uint8 *child_end_ptr;
                
                skip_element(&temp);
                child_end_ptr = temp.ptr;
                
                /* Type filtering for small lists too */
                if (child_stype == elem_stype || child_stype == SENTRY_TYPE_LIST)
                {
                    if (contains_fast_scan(child_start, child_end_ptr,
                                           container_syms, container_sym_lens,
                                           container_sym_hashes, container_sym_count,
                                           elem_ptr, elem_end, elem_hash,
                                           elem_syms, elem_sym_lens,
                                           elem_sym_hashes, elem_sym_count,
                                           elem_first_byte, elem_stype))
                        return true;
                }
            }
        }
    }
    
    return false;
}

/*
 * sexp_compute_hash - Compute structural hash of a sexp
 * Useful for debugging and for elements without stored hashes
 */
uint32
sexp_compute_hash(Sexp *sexp)
{
    SexpReadState state;
    uint32 hash;
    
    sexp_init_read_state(&state, sexp);
    hash = sexp_element_hash(state.ptr, state.end, 
                             state.symbols, state.sym_lengths, state.sym_count);
    sexp_free_read_state(&state);
    
    return hash;
}

/*
 * sexp_compute_hash_packed - Compute structural hash from packed varlena
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 * Use this variant when argument comes from PG_GETARG_SEXP_PACKED.
 */
uint32
sexp_compute_hash_packed(struct varlena *packed)
{
    SexpReadState state;
    uint32 hash;
    
    sexp_init_read_state_packed(&state, packed);
    hash = sexp_element_hash(state.ptr, state.end, 
                             state.symbols, state.sym_lengths, state.sym_count);
    sexp_free_read_state(&state);
    
    return hash;
}

/*
 * ===========================================================================
 * BLOOM SIGNATURE COMPUTATION
 * ===========================================================================
 *
 * Bloom signatures encode "what elements are present" in a structure.
 * Used for fast rejection in containment checks:
 *   if (needle_bloom & ~container_bloom) != 0:
 *       needle definitely NOT contained (Bloom says no)
 *
 * Each element contributes its hash bits to the signature via bloom_compute_sig().
 * Lists combine all descendant signatures (union), so checking the list's bloom
 * also covers all nested elements.
 */

/*
 * sexp_element_bloom - Compute Bloom signature for an element
 * 
 * Returns a 64-bit Bloom signature encoding all values present in the element.
 * For atoms: signature from element hash
 * For lists: union of signatures from all descendants
 */
BloomSig
sexp_element_bloom(uint8 *ptr, uint8 *end, char **symbols, int *sym_lengths, int sym_count)
{
    uint8 byte;
    uint8 tag;
    uint32 elem_hash;
    
    if (ptr >= end)
        return 0;
    
    byte = *ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            /* NIL contributes a unique signature */
            return bloom_compute_sig(sexp_hash_uint32(SEXP_TAG_NIL));
            
        case SEXP_TAG_SMALLINT:
        {
            int64 val = (int)(byte & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
            uint32 type_hash = sexp_hash_uint32(SEXP_TAG_INTEGER);
            uint32 value_hash = sexp_hash_int64(val);
            elem_hash = hash_combine(type_hash, value_hash);
            return bloom_compute_sig(elem_hash);
        }
            
        case SEXP_TAG_INTEGER:
        {
            uint64 encoded = decode_varint(&ptr, end);
            int64 val = zigzag_decode(encoded);
            uint32 type_hash = sexp_hash_uint32(SEXP_TAG_INTEGER);
            uint32 value_hash = sexp_hash_int64(val);
            elem_hash = hash_combine(type_hash, value_hash);
            return bloom_compute_sig(elem_hash);
        }
        
        case SEXP_TAG_FLOAT:
        {
            double val;
            uint32 type_hash = sexp_hash_uint32(SEXP_TAG_FLOAT);
            uint32 value_hash;
            memcpy(&val, ptr, sizeof(double));
            value_hash = sexp_hash_float64(val);
            elem_hash = hash_combine(type_hash, value_hash);
            return bloom_compute_sig(elem_hash);
        }
            
        case SEXP_TAG_SYMBOL_REF:
        {
            uint64 idx = decode_varint(&ptr, end);
            if ((int)idx < sym_count)
            {
                elem_hash = sexp_hash_string_with_tag(SEXP_TAG_SYMBOL_REF, 
                                                      symbols[idx], sym_lengths[idx]);
                return bloom_compute_sig(elem_hash);
            }
            return 0;
        }
        
        case SEXP_TAG_SHORT_STRING:
        {
            int len = byte & SEXP_DATA_MASK;
            elem_hash = sexp_hash_string_with_tag(SEXP_TAG_SHORT_STRING, (char *)ptr, len);
            return bloom_compute_sig(elem_hash);
        }
        
        case SEXP_TAG_LONG_STRING:
        {
            uint64 len = decode_varint(&ptr, end);
            elem_hash = sexp_hash_string_with_tag(SEXP_TAG_SHORT_STRING, (char *)ptr, (int)len);
            return bloom_compute_sig(elem_hash);
        }
        
        case SEXP_TAG_LIST:
        {
            uint64 count = byte & SEXP_DATA_MASK;
            BloomSig list_bloom = 0;
            uint64 i;
            
            if (unlikely(count == 0))
            {
                /* Large list - read stored count, skip hash, compute from children */
                SEntry *sentries;
                uint8 *data_start;
                uint32 actual_count;
                
                actual_count = SEXP_LOAD_UINT32_UNALIGNED(ptr);
                ptr += sizeof(uint32);  /* Skip count */
                ptr += sizeof(uint32);  /* Skip stored hash */
                
                sentries = (SEntry *)ptr;
                data_start = ptr + actual_count * sizeof(SEntry);
                
                for (i = 0; i < actual_count; i++)
                {
                    uint8 *child_ptr = data_start + SENTRY_GET_OFFSET(sentries[i]);
                    uint8 *child_end = (i + 1 < actual_count) ? 
                        (data_start + SENTRY_GET_OFFSET(sentries[i + 1])) : end;
                    
                    list_bloom = bloom_combine(list_bloom, 
                        sexp_element_bloom(child_ptr, child_end, symbols, sym_lengths, sym_count));
                }
                
                /* Also add signature for the list itself (count + type) */
                elem_hash = hash_combine(sexp_hash_uint32(actual_count), 
                                         sexp_hash_uint32(SEXP_TAG_LIST));
                list_bloom = bloom_combine(list_bloom, bloom_compute_sig(elem_hash));
                return list_bloom;
            }
            
            /* Small list v6 - skip size prefix, compute from children */
            {
                SexpReadState temp;
                
                /* Skip the size prefix (varint) */
                (void)decode_varint(&ptr, end);
                
                for (i = 0; i < count; i++)
                {
                    list_bloom = bloom_combine(list_bloom, 
                        sexp_element_bloom(ptr, end, symbols, sym_lengths, sym_count));
                    
                    /* Skip element */
                    temp.ptr = ptr;
                    temp.end = end;
                    temp.data = ptr;
                    temp.symbols = NULL;
                    temp.sym_lengths = NULL;
                    temp.sym_hashes = NULL;
                    temp.sym_count = 0;
                    skip_element(&temp);
                    ptr = temp.ptr;
                }
                
                /* Also add signature for the list itself */
                elem_hash = hash_combine(sexp_hash_uint32((uint32)count), 
                                         sexp_hash_uint32(SEXP_TAG_LIST));
                list_bloom = bloom_combine(list_bloom, bloom_compute_sig(elem_hash));
                return list_bloom;
            }
        }
        
        default:
            return 0;
    }
}

/*
 * sexp_compute_bloom - Compute Bloom signature of a sexp
 * Entry point for computing Bloom signature from a Sexp datum.
 */
BloomSig
sexp_compute_bloom(Sexp *sexp)
{
    SexpReadState state;
    BloomSig bloom;
    
    sexp_init_read_state(&state, sexp);
    bloom = sexp_element_bloom(state.ptr, state.end, 
                               state.symbols, state.sym_lengths, state.sym_count);
    sexp_free_read_state(&state);
    
    return bloom;
}

/*
 * sexp_contains - Check if container contains element (recursive)
 * 
 * Optimizations:
 * - Type filtering: skips elements of incompatible types using SEntry
 * - First-byte quick check: avoids full comparison when types differ
 * - Bloom filter: fast rejection using Bloom signatures
 */
bool
sexp_contains(Sexp *container, Sexp *element)
{
    SexpReadState container_state;
    SexpReadState elem_state;
    uint8 *elem_data;
    uint8 *elem_end;
    uint8 elem_first_byte;
    uint32 elem_stype;
    uint32 elem_hash;
    BloomSig container_bloom;
    BloomSig elem_bloom;
    bool result;
    
    /* Initialize read states - needed for symbol tables */
    sexp_init_read_state(&container_state, container);
    sexp_init_read_state(&elem_state, element);
    
    if (container_state.ptr >= container_state.end)
    {
        sexp_free_read_state(&container_state);
        sexp_free_read_state(&elem_state);
        return false;
    }
    
    /* Get element info for fingerprint and type filtering */
    elem_data = elem_state.ptr;
    elem_end = elem_state.end;  /* Use the parsed end, no need to measure */
    elem_first_byte = *elem_data;
    elem_stype = get_sentry_type_from_byte(elem_first_byte);
    
    /*
     * BLOOM FILTER FAST REJECTION:
     * Compute Bloom signatures for both container and element.
     * If element's bits are not a subset of container's bits,
     * we know for certain the element cannot be contained.
     *
     * This check is cheap (O(n) traversal of each structure once)
     * and can avoid expensive recursive containment search.
     */
    container_bloom = sexp_element_bloom(
        container_state.ptr, container_state.end,
        container_state.symbols, container_state.sym_lengths, container_state.sym_count);
    elem_bloom = sexp_element_bloom(
        elem_data, elem_end,
        elem_state.symbols, elem_state.sym_lengths, elem_state.sym_count);
    
    if (!bloom_may_contain(container_bloom, elem_bloom))
    {
        /* Bloom says definitely NOT contained - fast path return */
        sexp_free_read_state(&container_state);
        sexp_free_read_state(&elem_state);
        return false;
    }
    
    /* 
     * Compute element hash for potential early rejection.
     * This is cheap for atoms, and for lists it's already stored.
     */
    elem_hash = sexp_element_hash(elem_data, elem_end,
                                  elem_state.symbols, elem_state.sym_lengths, 
                                  elem_state.sym_count);
    
    /* Use fast scan with type filtering and hash rejection */
    result = contains_fast_scan(
        container_state.ptr, container_state.end,
        container_state.symbols, container_state.sym_lengths,
        container_state.sym_hashes, container_state.sym_count,
        elem_data, elem_end, elem_hash,
        elem_state.symbols, elem_state.sym_lengths,
        elem_state.sym_hashes, elem_state.sym_count,
        elem_first_byte, elem_stype
    );
    
    sexp_free_read_state(&container_state);
    sexp_free_read_state(&elem_state);
    
    return result;
}

/*
 * sexp_contains_packed - Containment check for packed varlena
 * 
 * OPTIMIZED: Uses packed varlena to avoid unnecessary detoasting copy.
 * Use this variant when both arguments come from PG_GETARG_SEXP_PACKED.
 */
bool
sexp_contains_packed(struct varlena *container, struct varlena *element)
{
    SexpReadState container_state;
    SexpReadState elem_state;
    uint8 *elem_data;
    uint8 *elem_end;
    uint8 elem_first_byte;
    uint32 elem_stype;
    uint32 elem_hash;
    BloomSig container_bloom;
    BloomSig elem_bloom;
    bool result;
    
    /* Initialize read states - needed for symbol tables */
    sexp_init_read_state_packed(&container_state, container);
    sexp_init_read_state_packed(&elem_state, element);
    
    if (container_state.ptr >= container_state.end)
    {
        sexp_free_read_state(&container_state);
        sexp_free_read_state(&elem_state);
        return false;
    }
    
    /* Get element info for fingerprint and type filtering */
    elem_data = elem_state.ptr;
    elem_end = elem_state.end;
    elem_first_byte = *elem_data;
    elem_stype = get_sentry_type_from_byte(elem_first_byte);
    
    /* BLOOM FILTER FAST REJECTION */
    container_bloom = sexp_element_bloom(
        container_state.ptr, container_state.end,
        container_state.symbols, container_state.sym_lengths, container_state.sym_count);
    elem_bloom = sexp_element_bloom(
        elem_data, elem_end,
        elem_state.symbols, elem_state.sym_lengths, elem_state.sym_count);
    
    if (!bloom_may_contain(container_bloom, elem_bloom))
    {
        sexp_free_read_state(&container_state);
        sexp_free_read_state(&elem_state);
        return false;
    }
    
    elem_hash = sexp_element_hash(elem_data, elem_end,
                                  elem_state.symbols, elem_state.sym_lengths, 
                                  elem_state.sym_count);
    
    result = contains_fast_scan(
        container_state.ptr, container_state.end,
        container_state.symbols, container_state.sym_lengths,
        container_state.sym_hashes, container_state.sym_count,
        elem_data, elem_end, elem_hash,
        elem_state.symbols, elem_state.sym_lengths,
        elem_state.sym_hashes, elem_state.sym_count,
        elem_first_byte, elem_stype
    );
    
    sexp_free_read_state(&container_state);
    sexp_free_read_state(&elem_state);
    
    return result;
}

/*
 * ===========================================================================
 * KEY-BASED CONTAINMENT (@>>)
 * ===========================================================================
 *
 * Key-based containment treats s-expressions like JSON objects where:
 * - The first element (car) of a list acts as a "key" or "tag"
 * - Remaining elements are "values" that can be matched in any order
 *
 * Semantics:
 *   A @>> B means "A contains B in key-based sense"
 *
 * Rules:
 *   1. atom @>> atom: equal values
 *   2. A @>> atom: A contains that atom anywhere (same as @>)
 *   3. list @>> list:
 *      - Same head symbol (car)
 *      - For each element in needle's tail, container has matching element
 *      - Order independent (unlike structural @>)
 *      - Container may have extra elements
 *
 * Examples:
 *   (user (name "alice") (age 30)) @>> (user (age 30))         -- TRUE
 *   (user (name "alice") (age 30)) @>> (user (name "bob"))     -- FALSE
 *   (+ 1 2 3) @>> (+ 2 1)                                       -- TRUE (order independent!)
 *   (+ 1 2) @>> (+ 1 2 3)                                       -- FALSE (needle has more)
 */

/* Forward declaration */
static bool key_contains_recursive(
    SexpReadState *container_state, uint8 *container_ptr, uint8 *container_end,
    SexpReadState *needle_state, uint8 *needle_ptr, uint8 *needle_end);

/*
 * Check if a single element matches (for atoms) or contains (for lists)
 * Used for matching needle elements against container elements
 * 
 * OPTIMIZED: Takes pre-initialized state pointers, avoids allocation
 */
static inline bool
element_key_matches(
    SexpReadState *container_state, uint8 *container_ptr, uint8 *container_end,
    SexpReadState *needle_state, uint8 *needle_ptr, uint8 *needle_end)
{
    uint8 container_byte, needle_byte;
    uint8 container_tag, needle_tag;
    
    if (container_ptr >= container_end || needle_ptr >= needle_end)
        return false;
    
    container_byte = *container_ptr;
    needle_byte = *needle_ptr;
    container_tag = container_byte & SEXP_TAG_MASK;
    needle_tag = needle_byte & SEXP_TAG_MASK;
    
    /* If needle is an atom, check equality */
    if (needle_tag != SEXP_TAG_LIST)
    {
        /* Both must be same type and equal value */
        SexpReadState cs, ns;
        bool result;

        /* Quick rejection: different tags (except int variants) */
        if (container_tag != needle_tag)
        {
            /* Allow smallint vs integer comparison */
            if (!((container_tag == SEXP_TAG_SMALLINT || container_tag == SEXP_TAG_INTEGER) &&
                  (needle_tag == SEXP_TAG_SMALLINT || needle_tag == SEXP_TAG_INTEGER)))
                return false;
        }
        
        cs.ptr = container_ptr;
        cs.end = container_end;
        cs.data = container_state->data;
        cs.symbols = container_state->symbols;
        cs.sym_lengths = container_state->sym_lengths;
        cs.sym_hashes = container_state->sym_hashes;
        cs.sym_count = container_state->sym_count;
        
        ns.ptr = needle_ptr;
        ns.end = needle_end;
        ns.data = needle_state->data;
        ns.symbols = needle_state->symbols;
        ns.sym_lengths = needle_state->sym_lengths;
        ns.sym_hashes = needle_state->sym_hashes;
        ns.sym_count = needle_state->sym_count;
        
        result = elements_equal_recursive(&cs, &ns);
        return result;
    }
    
    /* Needle is a list - container must also be a list with key-based match */
    if (container_tag != SEXP_TAG_LIST)
        return false;
    
    return key_contains_recursive(container_state, container_ptr, container_end,
                                  needle_state, needle_ptr, needle_end);
}

/*
 * Key-based containment check for lists
 * 
 * For (head a b c) @>> (head x y):
 * - Heads must match (same symbol)
 * - For each x, y in needle tail, container must have matching element
 * - Order independent
 *
 * For small lists, direct iteration is faster than caching overhead.
 * For larger lists, use SEntry table when available for O(1) element access.
 */

/* Threshold below which direct iteration beats caching overhead */
#define CACHE_THRESHOLD 8

static bool
key_contains_recursive(
    SexpReadState *container_state, uint8 *container_ptr, uint8 *container_end,
    SexpReadState *needle_state, uint8 *needle_ptr, uint8 *needle_end)
{
    uint8 container_byte, needle_byte;
    uint64 container_count, needle_count;
    uint8 *container_data_start, *needle_data_start;
    SEntry *container_sentries = NULL, *needle_sentries = NULL;
    uint8 *cptr, *nptr;
    int ni, ci;
    
    if (container_ptr >= container_end || needle_ptr >= needle_end)
        return false;
    
    container_byte = *container_ptr;
    needle_byte = *needle_ptr;
    
    /* Both must be lists */
    if ((container_byte & SEXP_TAG_MASK) != SEXP_TAG_LIST ||
        (needle_byte & SEXP_TAG_MASK) != SEXP_TAG_LIST)
        return false;
    
    /* Decode container list header */
    cptr = container_ptr + 1;
    container_count = container_byte & SEXP_DATA_MASK;
    if (unlikely(container_count == 0))
    {
        uint32 cnt32;
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(cptr);
        cptr += sizeof(uint32);
        container_count = cnt32;
        cptr += sizeof(uint32);  /* Skip hash */
        container_sentries = (SEntry *)cptr;
        cptr += container_count * sizeof(SEntry);
    }
    else
    {
        /* Small list v6: skip size prefix */
        (void)decode_varint(&cptr, container_end);
    }
    container_data_start = cptr;
    
    /* Decode needle list header */
    nptr = needle_ptr + 1;
    needle_count = needle_byte & SEXP_DATA_MASK;
    if (unlikely(needle_count == 0))
    {
        uint32 cnt32;
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(nptr);
        nptr += sizeof(uint32);
        needle_count = cnt32;
        nptr += sizeof(uint32);  /* Skip hash */
        needle_sentries = (SEntry *)nptr;
        nptr += needle_count * sizeof(SEntry);
    }
    else
    {
        /* Small list v6: skip size prefix */
        (void)decode_varint(&nptr, needle_end);
    }
    needle_data_start = nptr;
    
    /* Empty needle matches anything */
    if (needle_count == 0)
        return true;
    
    /* Container must have at least as many elements */
    if (container_count < needle_count)
        return false;
    
    /* Compare heads - must be equal */
    {
        SexpReadState cs, ns;
        uint8 *c_head_start, *c_head_end, *n_head_start, *n_head_end;
        
        /* Get head element bounds */
        if (container_sentries)
        {
            c_head_start = container_data_start + SENTRY_GET_OFFSET(container_sentries[0]);
            c_head_end = (container_count > 1) ? 
                (container_data_start + SENTRY_GET_OFFSET(container_sentries[1])) : container_end;
        }
        else
        {
            c_head_start = container_data_start;
            c_head_end = sexp_skip_element(c_head_start, container_end);
        }
        
        if (needle_sentries)
        {
            n_head_start = needle_data_start + SENTRY_GET_OFFSET(needle_sentries[0]);
            n_head_end = (needle_count > 1) ?
                (needle_data_start + SENTRY_GET_OFFSET(needle_sentries[1])) : needle_end;
        }
        else
        {
            n_head_start = needle_data_start;
            n_head_end = sexp_skip_element(n_head_start, needle_end);
        }
        
        cs.ptr = c_head_start;
        cs.end = c_head_end;
        cs.data = container_state->data;
        cs.symbols = container_state->symbols;
        cs.sym_lengths = container_state->sym_lengths;
        cs.sym_count = container_state->sym_count;
        cs.sym_hashes = container_state->sym_hashes;
        
        ns.ptr = n_head_start;
        ns.end = n_head_end;
        ns.data = needle_state->data;
        ns.symbols = needle_state->symbols;
        ns.sym_lengths = needle_state->sym_lengths;
        ns.sym_count = needle_state->sym_count;
        ns.sym_hashes = needle_state->sym_hashes;
        
        if (!elements_equal_recursive(&cs, &ns))
            return false;
    }
    
    /* If needle only has head, match */
    if (needle_count == 1)
        return true;
    
    /*
     * For each element in needle tail, find match in container tail.
     * Use direct iteration - simpler and fast for typical small lists.
     * SEntry tables give us O(1) element access when available.
     */
    for (ni = 1; ni < (int)needle_count; ni++)
    {
        bool found = false;
        uint8 *n_start, *n_end;
        uint8 n_tag;
        
        /* Get needle element bounds */
        if (needle_sentries)
        {
            n_start = needle_data_start + SENTRY_GET_OFFSET(needle_sentries[ni]);
            n_end = (ni + 1 < (int)needle_count) ?
                (needle_data_start + SENTRY_GET_OFFSET(needle_sentries[ni + 1])) : needle_end;
        }
        else
        {
            /* For small lists without SEntry, scan to find element */
            SexpReadState temp;
            int j;
            temp.ptr = needle_data_start;
            temp.end = needle_end;
            temp.data = needle_data_start;
            temp.symbols = NULL;
            temp.sym_lengths = NULL;
            temp.sym_hashes = NULL;
            temp.sym_count = 0;
            for (j = 0; j < ni; j++)
                skip_element(&temp);
            n_start = temp.ptr;
            skip_element(&temp);
            n_end = temp.ptr;
        }
        
        n_tag = (*n_start) & SEXP_TAG_MASK;
        
        /* Search for match in container tail */
        for (ci = 1; ci < (int)container_count && !found; ci++)
        {
            uint8 *c_start, *c_end;
            uint8 c_tag;
            
            /* Get container element bounds */
            if (container_sentries)
            {
                c_start = container_data_start + SENTRY_GET_OFFSET(container_sentries[ci]);
                c_end = (ci + 1 < (int)container_count) ?
                    (container_data_start + SENTRY_GET_OFFSET(container_sentries[ci + 1])) : container_end;
            }
            else
            {
                /* For small lists without SEntry, scan to find element */
                SexpReadState temp;
                int j;
                temp.ptr = container_data_start;
                temp.end = container_end;
                temp.data = container_data_start;
                temp.symbols = NULL;
                temp.sym_lengths = NULL;
                temp.sym_hashes = NULL;
                temp.sym_count = 0;
                for (j = 0; j < ci; j++)
                    skip_element(&temp);
                c_start = temp.ptr;
                skip_element(&temp);
                c_end = temp.ptr;
            }
            
            c_tag = (*c_start) & SEXP_TAG_MASK;
            
            /* Quick type rejection */
            if (n_tag != SEXP_TAG_LIST && c_tag != n_tag)
            {
                /* Allow smallint/integer cross-comparison */
                if (!((c_tag == SEXP_TAG_SMALLINT || c_tag == SEXP_TAG_INTEGER) &&
                      (n_tag == SEXP_TAG_SMALLINT || n_tag == SEXP_TAG_INTEGER)))
                    continue;
            }
            
            /* Full comparison */
            if (element_key_matches(container_state, c_start, c_end,
                                    needle_state, n_start, n_end))
            {
                found = true;
            }
        }
        
        if (!found)
            return false;
    }
    
    return true;
}

/*
 * Search for needle anywhere in container using key-based matching.
 * 
 * Uses type filtering to skip non-matching branches early.
 * Avoids repeated SexpReadState initialization in hot path.
 */
static bool
contains_key_search(
    SexpReadState *container_state, uint8 *container_ptr, uint8 *container_end,
    SexpReadState *needle_state, uint8 *needle_ptr, uint8 *needle_end,
    uint8 needle_tag)
{
    uint8 container_byte;
    uint8 container_tag;
    
    if (unlikely(container_ptr >= container_end))
        return false;
    
    container_byte = *container_ptr;
    container_tag = container_byte & SEXP_TAG_MASK;
    
    /* If needle is an atom, check for direct equality */
    if (needle_tag != SEXP_TAG_LIST)
    {
        /* 
         * OPTIMIZATION: Quick type check before full comparison.
         * For atoms, the container element must have compatible type.
         */
        if (container_tag == needle_tag ||
            /* Allow smallint/integer cross-comparison */
            ((container_tag == SEXP_TAG_SMALLINT || container_tag == SEXP_TAG_INTEGER) &&
             (needle_tag == SEXP_TAG_SMALLINT || needle_tag == SEXP_TAG_INTEGER)))
        {
            /* Use optimized direct comparison for atoms */
            if (atom_compare_direct(container_ptr, container_end,
                                    container_state->symbols, container_state->sym_lengths,
                                    container_state->sym_hashes, container_state->sym_count,
                                    needle_ptr, needle_end,
                                    needle_state->symbols, needle_state->sym_lengths,
                                    needle_state->sym_hashes, needle_state->sym_count))
                return true;
        }
    }
    else if (container_tag == SEXP_TAG_LIST)
    {
        /* Both are lists - check key-based containment at this level */
        if (key_contains_recursive(container_state, container_ptr, container_end,
                                   needle_state, needle_ptr, needle_end))
            return true;
    }
    
    /* Recurse into container if it's a list */
    if (container_tag == SEXP_TAG_LIST)
    {
        uint8 *ptr = container_ptr + 1;
        uint64 count;
        SEntry *sentries = NULL;
        uint8 *data_start;
        uint64 i;
        
        count = container_byte & SEXP_DATA_MASK;
        if (unlikely(count == 0))
        {
            uint32 cnt32;
            cnt32 = SEXP_LOAD_UINT32_UNALIGNED(ptr);
            ptr += sizeof(uint32);
            count = cnt32;
            ptr += sizeof(uint32);  /* Skip hash */
            sentries = (SEntry *)ptr;
            ptr += count * sizeof(SEntry);
        }
        else
        {
            /* Small list v6: skip size prefix */
            (void)decode_varint(&ptr, container_end);
        }
        data_start = ptr;
        
        /* Use SEntry for efficient iteration when available */
        if (sentries)
        {
            /*
             * OPTIMIZATION: For atom needles, use type filtering.
             * Skip children that cannot possibly match based on SEntry type.
             */
            if (needle_tag != SEXP_TAG_LIST)
            {
                uint32 needle_stype = get_sentry_type_from_byte(needle_tag);
                
                for (i = 0; i < count; i++)
                {
                    uint32 child_stype = SENTRY_GET_TYPE(sentries[i]);
                    
                    /* Only recurse if child could contain the atom */
                    if (child_stype == needle_stype || child_stype == SENTRY_TYPE_LIST)
                    {
                        uint8 *elem_start = data_start + SENTRY_GET_OFFSET(sentries[i]);
                        uint8 *elem_end = (i + 1 < count) ?
                            (data_start + SENTRY_GET_OFFSET(sentries[i + 1])) : container_end;
                        
                        if (contains_key_search(container_state, elem_start, elem_end,
                                                needle_state, needle_ptr, needle_end,
                                                needle_tag))
                            return true;
                    }
                }
            }
            else
            {
                /* Needle is a list - only search in list children */
                for (i = 0; i < count; i++)
                {
                    uint32 child_stype = SENTRY_GET_TYPE(sentries[i]);
                    
                    if (child_stype == SENTRY_TYPE_LIST)
                    {
                        uint8 *elem_start = data_start + SENTRY_GET_OFFSET(sentries[i]);
                        uint8 *elem_end = (i + 1 < count) ?
                            (data_start + SENTRY_GET_OFFSET(sentries[i + 1])) : container_end;
                        
                        if (contains_key_search(container_state, elem_start, elem_end,
                                                needle_state, needle_ptr, needle_end,
                                                needle_tag))
                            return true;
                    }
                }
            }
        }
        else
        {
            /* Small list without SEntry - sequential scan */
            SexpReadState temp;
            temp.ptr = ptr;
            temp.end = container_end;
            temp.data = ptr;
            temp.symbols = NULL;
            temp.sym_lengths = NULL;
            temp.sym_hashes = NULL;
            temp.sym_count = 0;
            
            for (i = 0; i < count; i++)
            {
                uint8 *elem_start = temp.ptr;
                uint8 *elem_end;
                uint8 child_tag = *elem_start & SEXP_TAG_MASK;
                
                skip_element(&temp);
                elem_end = temp.ptr;
                
                /* Type filtering for small lists too */
                if (needle_tag != SEXP_TAG_LIST)
                {
                    if (child_tag == needle_tag || child_tag == SEXP_TAG_LIST ||
                        ((child_tag == SEXP_TAG_SMALLINT || child_tag == SEXP_TAG_INTEGER) &&
                         (needle_tag == SEXP_TAG_SMALLINT || needle_tag == SEXP_TAG_INTEGER)))
                    {
                        if (contains_key_search(container_state, elem_start, elem_end,
                                                needle_state, needle_ptr, needle_end,
                                                needle_tag))
                            return true;
                    }
                }
                else if (child_tag == SEXP_TAG_LIST)
                {
                    if (contains_key_search(container_state, elem_start, elem_end,
                                            needle_state, needle_ptr, needle_end,
                                            needle_tag))
                        return true;
                }
            }
        }
    }
    
    return false;
}

/*
 * sexp_contains_key - Key-based containment check (@>>)
 *
 * Like jsonb @>, matches list heads as "keys" and allows
 * order-independent matching of list contents.
 * 
 * Uses Bloom filter for fast rejection and type filtering
 * to skip non-matching branches.
 */
bool
sexp_contains_key(Sexp *container, Sexp *needle)
{
    SexpReadState container_state;
    SexpReadState needle_state;
    uint8 needle_tag;
    BloomSig container_bloom;
    BloomSig needle_bloom;
    bool result;
    
    sexp_init_read_state(&container_state, container);
    sexp_init_read_state(&needle_state, needle);
    
    if (container_state.ptr >= container_state.end ||
        needle_state.ptr >= needle_state.end)
    {
        sexp_free_read_state(&container_state);
        sexp_free_read_state(&needle_state);
        return false;
    }
    
    /*
     * BLOOM FILTER FAST REJECTION:
     * If needle's Bloom bits are not a subset of container's bits,
     * the needle cannot be contained. This is a cheap O(n) check
     * that can avoid expensive recursive search.
     */
    container_bloom = sexp_element_bloom(
        container_state.ptr, container_state.end,
        container_state.symbols, container_state.sym_lengths, container_state.sym_count);
    needle_bloom = sexp_element_bloom(
        needle_state.ptr, needle_state.end,
        needle_state.symbols, needle_state.sym_lengths, needle_state.sym_count);
    
    if (!bloom_may_contain(container_bloom, needle_bloom))
    {
        /* Bloom says definitely NOT contained - fast path return */
        sexp_free_read_state(&container_state);
        sexp_free_read_state(&needle_state);
        return false;
    }
    
    needle_tag = (*needle_state.ptr) & SEXP_TAG_MASK;
    
    result = contains_key_search(
        &container_state, container_state.ptr, container_state.end,
        &needle_state, needle_state.ptr, needle_state.end,
        needle_tag);
    
    sexp_free_read_state(&container_state);
    sexp_free_read_state(&needle_state);
    
    return result;
}
