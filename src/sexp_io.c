/*
 * sexp_io.c
 *
 * S-expression I/O functions - converts binary format to text representation
 */

#include "pg_sexp.h"
#include "sexp_debug.h"
#include "utils/memutils.h"
#include <string.h>
#include <math.h>

/* Forward declarations */
static void output_element(SexpReadState *state, StringInfo buf);
static void init_read_state_internal(SexpReadState *state, uint8 *data, int len);

/* Singleton NIL sexp - allocated once in TopMemoryContext */
static Sexp *nil_singleton = NULL;

/*
 * sexp_get_nil_singleton - Return cached NIL singleton
 * 
 * Thread-safe via PostgreSQL's single-threaded model.
 * Allocated in TopMemoryContext to survive transaction boundaries.
 */
Sexp *
sexp_get_nil_singleton(void)
{
    if (nil_singleton == NULL)
    {
        MemoryContext old_ctx;
        int total_size;
        
        old_ctx = MemoryContextSwitchTo(TopMemoryContext);
        
        /* nil = version(1) + sym_count(1, varint 0) + nil_tag(1) */
        total_size = VARHDRSZ + 3;
        nil_singleton = (Sexp *) palloc(total_size);
        SET_VARSIZE(nil_singleton, total_size);
        nil_singleton->version = SEXP_FORMAT_VERSION;
        nil_singleton->data[0] = 0;  /* 0 symbols (varint) */
        nil_singleton->data[1] = SEXP_TAG_NIL;
        
        MemoryContextSwitchTo(old_ctx);
    }
    return nil_singleton;
}

/*
 * Initialize read state from Sexp
 */
void
sexp_init_read_state(SexpReadState *state, Sexp *sexp)
{
    uint8 *data = (uint8 *)&sexp->version;
    int len = VARSIZE(sexp) - VARHDRSZ;
    init_read_state_internal(state, data, len);
}

/*
 * Initialize read state from packed varlena (for PG_DETOAST_DATUM_PACKED)
 * 
 * IMPORTANT: Use this for read-only operations to avoid unnecessary copying.
 * The packed argument may be a short varlena, so we must use VARDATA_ANY
 * and VARSIZE_ANY_EXHDR instead of VARDATA/VARSIZE.
 */
void
sexp_init_read_state_packed(SexpReadState *state, struct varlena *packed)
{
    uint8 *data = (uint8 *)VARDATA_ANY(packed);
    int len = VARSIZE_ANY_EXHDR(packed);
    init_read_state_internal(state, data, len);
}

/*
 * Initialize read state from raw data
 */
static void
init_read_state_internal(SexpReadState *state, uint8 *data, int len)
{
    int i;
    uint64 sym_count;
    
    state->data = data;
    state->end = data + len;
    state->ptr = data;
    
    /* Validate and skip version byte */
    SEXP_CHECK_VERSION(data[0]);
    state->ptr++;
    
    /* Read symbol table */
    sym_count = decode_varint(&state->ptr, state->end);
    state->sym_count = (int)sym_count;
    
    if (state->sym_count > 0)
    {
        /* Use stack arrays for small symbol tables to avoid palloc overhead */
        if (state->sym_count <= SEXP_SMALL_SYMTAB_SIZE)
        {
            state->symbols = state->stack_symbols;
            state->sym_lengths = state->stack_lengths;
            state->sym_hashes = state->stack_hashes;
            state->use_stack = true;
        }
        else
        {
            state->symbols = palloc(sizeof(char *) * state->sym_count);
            state->sym_lengths = palloc(sizeof(int) * state->sym_count);
            state->sym_hashes = palloc(sizeof(uint32) * state->sym_count);
            state->use_stack = false;
        }
        
        for (i = 0; i < state->sym_count; i++)
        {
            uint64 slen = decode_varint(&state->ptr, state->end);
            state->sym_lengths[i] = (int)slen;
            state->symbols[i] = (char *)state->ptr;
            /* Pre-compute symbol hash for fast comparison using PostgreSQL hash_bytes */
            state->sym_hashes[i] = hash_bytes((const unsigned char *)state->ptr, (int)slen);
            state->ptr += slen;
        }
    }
    else
    {
        state->symbols = NULL;
        state->sym_lengths = NULL;
        state->sym_hashes = NULL;
        state->use_stack = true;  /* No allocation needed */
    }
}

/*
 * Free read state resources
 */
void
sexp_free_read_state(SexpReadState *state)
{
    /* Only free if we allocated (not using stack arrays) */
    if (!state->use_stack)
    {
        if (state->symbols)
            pfree(state->symbols);
        if (state->sym_lengths)
            pfree(state->sym_lengths);
        if (state->sym_hashes)
            pfree(state->sym_hashes);
    }
}

/*
 * Get the type of the current element
 */
SexpType
sexp_read_type(SexpReadState *state)
{
    uint8 tag;
    
    if (state->ptr >= state->end)
        return SEXP_NIL;
    
    tag = *state->ptr & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            return SEXP_NIL;
        case SEXP_TAG_SMALLINT:
        case SEXP_TAG_INTEGER:
            return SEXP_INTEGER;
        case SEXP_TAG_FLOAT:
            return SEXP_FLOAT;
        case SEXP_TAG_SYMBOL_REF:
            return SEXP_SYMBOL;
        case SEXP_TAG_SHORT_STRING:
        case SEXP_TAG_LONG_STRING:
            return SEXP_STRING;
        case SEXP_TAG_LIST:
            return SEXP_LIST;
        default:
            return SEXP_NIL;
    }
}

/*
 * sexp_get_type - Get type of a sexp without needing external state
 */
SexpType
sexp_get_type(Sexp *sexp)
{
    SexpReadState state;
    SexpType type;
    
    sexp_init_read_state(&state, sexp);
    type = sexp_read_type(&state);
    sexp_free_read_state(&state);
    
    return type;
}

/*
 * sexp_get_type_packed - Get type from packed varlena (for PG_DETOAST_DATUM_PACKED)
 * 
 * This is a fast path for type checking that avoids full detoasting.
 * Use with PG_GETARG_SEXP_PACKED() for read-only type inspections.
 */
SexpType
sexp_get_type_packed(struct varlena *packed)
{
    SexpReadState state;
    SexpType type;
    
    sexp_init_read_state_packed(&state, packed);
    type = sexp_read_type(&state);
    sexp_free_read_state(&state);
    
    return type;
}

/*
 * sexp_to_cstring - Convert sexp to C string representation
 */
char *
sexp_to_cstring(Sexp *sexp)
{
    StringInfoData buf;
    SexpReadState state;
    
    initStringInfo(&buf);
    sexp_init_read_state(&state, sexp);
    
    output_element(&state, &buf);
    
    sexp_free_read_state(&state);
    
    return buf.data;
}

/*
 * sexp_to_string_internal - For compatibility
 */
void
sexp_to_string_internal(Sexp *sexp, char *data, int len, StringInfo buf)
{
    SexpReadState state;
    
    sexp_init_read_state(&state, sexp);
    output_element(&state, buf);
    sexp_free_read_state(&state);
}

/*
 * Output a single element
 */
static void
output_element(SexpReadState *state, StringInfo buf)
{
    uint8 byte;
    uint8 tag;
    
    if (state->ptr >= state->end)
    {
        appendStringInfoString(buf, "()");
        return;
    }
    
    byte = *state->ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            appendStringInfoString(buf, "()");
            break;
            
        case SEXP_TAG_SMALLINT:
        {
            int val = (int)(byte & SEXP_DATA_MASK) - SEXP_SMALLINT_BIAS;
            appendStringInfo(buf, "%d", val);
            break;
        }
        
        case SEXP_TAG_INTEGER:
        {
            uint64 encoded = decode_varint(&state->ptr, state->end);
            int64 val = zigzag_decode(encoded);
            appendStringInfo(buf, "%lld", (long long)val);
            break;
        }
        
        case SEXP_TAG_FLOAT:
        {
            float8 val;
            memcpy(&val, state->ptr, sizeof(float8));
            state->ptr += sizeof(float8);
            if (isnan(val))
                appendStringInfoString(buf, "nan");
            else if (isinf(val))
                appendStringInfoString(buf, val > 0 ? "inf" : "-inf");
            else
                appendStringInfo(buf, "%.17g", val);
            break;
        }
        
        case SEXP_TAG_SYMBOL_REF:
        {
            uint64 idx = decode_varint(&state->ptr, state->end);
            if ((int)idx < state->sym_count)
            {
                appendBinaryStringInfo(buf, state->symbols[idx], 
                                       state->sym_lengths[idx]);
            }
            else
            {
                appendStringInfoString(buf, "?invalid-symbol?");
            }
            break;
        }
        
        case SEXP_TAG_SHORT_STRING:
        {
            int slen = byte & SEXP_DATA_MASK;
            int i;
            
            appendStringInfoChar(buf, '"');
            for (i = 0; i < slen && state->ptr < state->end; i++)
            {
                char c = *state->ptr++;
                switch (c)
                {
                    case '\n': appendStringInfoString(buf, "\\n"); break;
                    case '\t': appendStringInfoString(buf, "\\t"); break;
                    case '\r': appendStringInfoString(buf, "\\r"); break;
                    case '\\': appendStringInfoString(buf, "\\\\"); break;
                    case '"':  appendStringInfoString(buf, "\\\""); break;
                    default:   appendStringInfoChar(buf, c); break;
                }
            }
            appendStringInfoChar(buf, '"');
            break;
        }
        
        case SEXP_TAG_LONG_STRING:
        {
            uint64 slen = decode_varint(&state->ptr, state->end);
            uint64 i;
            
            appendStringInfoChar(buf, '"');
            for (i = 0; i < slen && state->ptr < state->end; i++)
            {
                char c = *state->ptr++;
                switch (c)
                {
                    case '\n': appendStringInfoString(buf, "\\n"); break;
                    case '\t': appendStringInfoString(buf, "\\t"); break;
                    case '\r': appendStringInfoString(buf, "\\r"); break;
                    case '\\': appendStringInfoString(buf, "\\\\"); break;
                    case '"':  appendStringInfoString(buf, "\\\""); break;
                    default:   appendStringInfoChar(buf, c); break;
                }
            }
            appendStringInfoChar(buf, '"');
            break;
        }
        
        case SEXP_TAG_LIST:
        {
            uint64 count;
            uint64 i;
            
            /* Check if small list (count in tag byte) or large list */
            count = byte & SEXP_DATA_MASK;
            if (unlikely(count == 0))
            {
                /* Large list with SEntry table (v5/v6 format: count + hash + sentries) */
                uint32 cnt32;
                cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state->ptr);
                state->ptr += sizeof(uint32);
                count = cnt32;
                
                /* Skip structural hash */
                state->ptr += sizeof(uint32);
                
                /* Skip SEntry table (count * sizeof(SEntry)) */
                state->ptr += count * sizeof(SEntry);
            }
            else
            {
                /* Small list v6 format: skip size prefix (varint) */
                (void)decode_varint(&state->ptr, state->end);
            }
            
            appendStringInfoChar(buf, '(');
            for (i = 0; i < count; i++)
            {
                if (i > 0)
                    appendStringInfoChar(buf, ' ');
                output_element(state, buf);
            }
            appendStringInfoChar(buf, ')');
            break;
        }
        
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_CORRUPTED),
                     errmsg("invalid sexp tag: %d", tag)));
            break;
    }
}
