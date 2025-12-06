/*
 * sexp_parser.c
 *
 * S-expression parser - converts text to optimized binary format
 *
 * The parser builds a symbol table incrementally while parsing,
 * using a hash table for O(1) symbol interning instead of O(n) linear search.
 * This is critical for symbol-rich inputs where the same symbol appears many times.
 *
 * Output format:
 *   [version][symbol_table][root_element]
 *
 * Lists are encoded with:
 *   - Small lists (<=4 elements): compact format with payload size for O(1) skip
 *   - Large lists (>4 elements): SEntry table + structural hash for O(1) access
 */

#include "pg_sexp.h"
#include "sexp_debug.h"
#include <ctype.h>
#include <string.h>

/* Forward declarations */
static void skip_whitespace(SexpParseState *state);
static uint32 parse_list(SexpParseState *state);
static uint32 parse_atom(SexpParseState *state);
static uint32 parse_string(SexpParseState *state);
static uint32 parse_number_or_symbol(SexpParseState *state);
static int intern_symbol(SexpParseState *state, const char *sym, int len);
static void write_varint(StringInfo buf, uint64 value);
static uint32 sexp_parse_value_with_hash(SexpParseState *state);

/*
 * Initialize symbol table with hash table for O(1) lookup
 */
static void
init_symbol_table(SexpSymbolTable *tab)
{
    int i;
    
    tab->capacity = 32;
    tab->count = 0;
    tab->symbols = palloc(sizeof(char *) * tab->capacity);
    tab->lengths = palloc(sizeof(int) * tab->capacity);
    tab->hashes = palloc(sizeof(uint32) * tab->capacity);
    
    /* Initialize hash table */
    tab->hash_size = SYMTAB_INITIAL_HASH_SIZE;
    tab->hash_mask = tab->hash_size - 1;
    tab->hash_table = palloc(sizeof(int) * tab->hash_size);
    for (i = 0; i < tab->hash_size; i++)
        tab->hash_table[i] = SYMTAB_HASH_EMPTY;
}

/*
 * Free symbol table
 */
static void
free_symbol_table(SexpSymbolTable *tab)
{
    int i;
    for (i = 0; i < tab->count; i++)
        pfree(tab->symbols[i]);
    pfree(tab->symbols);
    pfree(tab->lengths);
    pfree(tab->hashes);
    pfree(tab->hash_table);
}

/*
 * Compute hash of a symbol string using PostgreSQL's hash_bytes
 */
static inline uint32
symbol_hash(const char *sym, int len)
{
    return hash_bytes((const unsigned char *)sym, len);
}

/*
 * Grow the hash table when load factor exceeds 0.5
 * This rehashes all existing symbols
 */
static void
grow_symbol_hash_table(SexpSymbolTable *tab)
{
    int new_size = tab->hash_size * 2;
    int new_mask = new_size - 1;
    int *new_table = palloc(sizeof(int) * new_size);
    int i;
    
    /* Initialize new table */
    for (i = 0; i < new_size; i++)
        new_table[i] = SYMTAB_HASH_EMPTY;
    
    /* Rehash all existing symbols */
    for (i = 0; i < tab->count; i++)
    {
        uint32 hash = tab->hashes[i];
        int slot = hash & new_mask;
        
        /* Linear probing to find empty slot */
        while (new_table[slot] != SYMTAB_HASH_EMPTY)
            slot = (slot + 1) & new_mask;
        
        new_table[slot] = i;
    }
    
    pfree(tab->hash_table);
    tab->hash_table = new_table;
    tab->hash_size = new_size;
    tab->hash_mask = new_mask;
}

/*
 * Intern a symbol - returns index in symbol table
 * Uses hash table for O(1) average-case lookup instead of O(n) linear search
 */
static int
intern_symbol(SexpParseState *state, const char *sym, int len)
{
    SexpSymbolTable *tab = &state->symtab;
    uint32 hash = symbol_hash(sym, len);
    int slot = hash & tab->hash_mask;
    int idx;
    
    /* Hash table lookup with linear probing */
    while ((idx = tab->hash_table[slot]) != SYMTAB_HASH_EMPTY)
    {
        /* Check if this is our symbol (hash match + length match + content match) */
        if (tab->hashes[idx] == hash && 
            tab->lengths[idx] == len && 
            memcmp(tab->symbols[idx], sym, len) == 0)
        {
            return idx;  /* Found existing symbol */
        }
        slot = (slot + 1) & tab->hash_mask;
    }
    
    /* Symbol not found - add new symbol */
    
    /* Grow symbol arrays if needed */
    if (tab->count >= tab->capacity)
    {
        tab->capacity *= 2;
        tab->symbols = repalloc(tab->symbols, sizeof(char *) * tab->capacity);
        tab->lengths = repalloc(tab->lengths, sizeof(int) * tab->capacity);
        tab->hashes = repalloc(tab->hashes, sizeof(uint32) * tab->capacity);
    }
    
    /* Grow hash table if load factor > 0.5 */
    if (tab->count * 2 >= tab->hash_size)
    {
        grow_symbol_hash_table(tab);
        /* Recompute slot after rehash */
        slot = hash & tab->hash_mask;
        while (tab->hash_table[slot] != SYMTAB_HASH_EMPTY)
            slot = (slot + 1) & tab->hash_mask;
    }
    
    /* Add new symbol */
    idx = tab->count++;
    tab->symbols[idx] = pnstrdup(sym, len);
    tab->lengths[idx] = len;
    tab->hashes[idx] = hash;
    tab->hash_table[slot] = idx;
    
    return idx;
}

/*
 * Write varint to StringInfo
 */
static void
write_varint(StringInfo buf, uint64 value)
{
    uint8 tmp[10];
    int len = encode_varint(tmp, value);
    appendBinaryStringInfo(buf, (char *)tmp, len);
}

/*
 * Write a signed integer using zigzag + varint
 */
static void
write_signed_varint(StringInfo buf, int64 value)
{
    write_varint(buf, zigzag_encode(value));
}

/*
 * sexp_parse - Main entry point for parsing
 */
Sexp *
sexp_parse(const char *input, int len)
{
    SexpParseState state;
    StringInfoData element_buf;
    StringInfoData final_buf;
    Sexp *result;
    int total_size;
    int i;
    
    /* Initialize parse state */
    state.input = input;
    state.ptr = input;
    state.end = input + len;
    state.depth = 0;
    init_symbol_table(&state.symtab);
    
    /* Buffer for the root element */
    initStringInfo(&element_buf);
    state.output = &element_buf;
    
    /* Skip leading whitespace */
    skip_whitespace(&state);
    
    if (state.ptr >= state.end)
    {
        /* Empty input - return nil */
        appendStringInfoChar(&element_buf, SEXP_TAG_NIL);
    }
    else
    {
        /* Parse the s-expression */
        sexp_parse_value(&state);
        
        /* Skip trailing whitespace */
        skip_whitespace(&state);
        
        /* Check for trailing garbage */
        if (state.ptr < state.end)
        {
            free_symbol_table(&state.symtab);
            pfree(element_buf.data);
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("unexpected characters after s-expression")));
        }
    }
    
    /* Build final output: version + symbol_table + element */
    initStringInfo(&final_buf);
    
    /* Version byte */
    appendStringInfoChar(&final_buf, SEXP_FORMAT_VERSION);
    
    /* Symbol table: count + entries */
    write_varint(&final_buf, state.symtab.count);
    for (i = 0; i < state.symtab.count; i++)
    {
        write_varint(&final_buf, state.symtab.lengths[i]);
        appendBinaryStringInfo(&final_buf, state.symtab.symbols[i], 
                               state.symtab.lengths[i]);
    }
    
    /* Append element data */
    appendBinaryStringInfo(&final_buf, element_buf.data, element_buf.len);
    
    /* Create the result Sexp */
    total_size = VARHDRSZ + final_buf.len;
    result = (Sexp *) palloc(total_size);
    SET_VARSIZE(result, total_size);
    memcpy(&result->version, final_buf.data, final_buf.len);
    
    /* Cleanup */
    free_symbol_table(&state.symtab);
    pfree(element_buf.data);
    pfree(final_buf.data);
    
    return result;
}

/*
 * sexp_parse_value - Parse a single s-expression value (legacy wrapper)
 */
void
sexp_parse_value(SexpParseState *state)
{
    (void)sexp_parse_value_with_hash(state);
}

/*
 * sexp_parse_value_with_hash - Parse a single s-expression value and return its hash
 * Returns: 32-bit structural hash of the parsed element
 */
static uint32
sexp_parse_value_with_hash(SexpParseState *state)
{
    skip_whitespace(state);
    
    if (state->ptr >= state->end)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("unexpected end of input")));
    }
    
    if (*state->ptr == '(')
    {
        return parse_list(state);
    }
    else if (*state->ptr == '"')
    {
        return parse_string(state);
    }
    else
    {
        return parse_atom(state);
    }
}

/*
 * skip_whitespace - Skip spaces, tabs, newlines, and comments
 */
static void
skip_whitespace(SexpParseState *state)
{
    while (state->ptr < state->end)
    {
        if (isspace((unsigned char) *state->ptr))
        {
            state->ptr++;
        }
        else if (*state->ptr == ';')
        {
            /* Skip line comment */
            while (state->ptr < state->end && *state->ptr != '\n')
                state->ptr++;
        }
        else
        {
            break;
        }
    }
}

/*
 * Helper: Get SEntry type from element tag byte
 */
static uint32
get_sentry_type_from_tag(uint8 tag_byte)
{
    uint8 tag = tag_byte & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
            return SENTRY_TYPE_NIL;
        case SEXP_TAG_SMALLINT:
        case SEXP_TAG_INTEGER:
            return SENTRY_TYPE_INTEGER;
        case SEXP_TAG_FLOAT:
            return SENTRY_TYPE_FLOAT;
        case SEXP_TAG_SYMBOL_REF:
            return SENTRY_TYPE_SYMBOL;
        case SEXP_TAG_SHORT_STRING:
        case SEXP_TAG_LONG_STRING:
            return SENTRY_TYPE_STRING;
        case SEXP_TAG_LIST:
            return SENTRY_TYPE_LIST;
        default:
            return SENTRY_TYPE_NIL;
    }
}

/*
 * parse_list - Parse a list: ( elem1 elem2 ... )
 *
 * List format with SEntry table and structural hash:
 *   [SEXP_TAG_LIST | small_count]  -- 1 byte, count in lower bits if <= 4
 *   [varint: payload_size]         -- for O(1) skip
 *   [element data...]              -- sequential elements
 *
 *   [SEXP_TAG_LIST]                -- 1 byte with 0 in data bits (large list)
 *   [uint32: count]                -- element count
 *   [uint32: structural_hash]      -- combined hash of all children
 *   [SEntry[0..count-1]]           -- SEntry table (type + offset per element)
 *   [element data...]              -- element data
 *
 * The structural hash enables quick containment rejection:
 * if hash bits don't match, element is definitely NOT contained.
 *
 * Returns: structural hash of the list
 */
static uint32
parse_list(SexpParseState *state)
{
    StringInfoData elements;
    StringInfo saved_output;
    int count = 0;
    int capacity = 16;
    SEntry *sentries;
    uint32 *child_hashes;
    uint32 list_hash;
    int i;
    
    /* Check depth limit using defensive macro */
    SEXP_CHECK_DEPTH(state->depth);
    
    state->depth++;
    
    /* Skip opening paren */
    Assert(*state->ptr == '(');
    state->ptr++;
    
    skip_whitespace(state);
    
    /* Check for empty list / nil */
    if (state->ptr < state->end && *state->ptr == ')')
    {
        state->ptr++;
        state->depth--;
        appendStringInfoChar(state->output, SEXP_TAG_NIL);
        return 0;  /* NIL has hash 0 */
    }
    
    /* Parse elements into temporary buffer, tracking offsets, types, and hashes */
    initStringInfo(&elements);
    saved_output = state->output;
    state->output = &elements;
    
    sentries = palloc(sizeof(SEntry) * capacity);
    child_hashes = palloc(sizeof(uint32) * capacity);
    
    while (state->ptr < state->end && *state->ptr != ')')
    {
        int elem_start;
        uint8 first_byte;
        uint32 sentry_type;
        uint32 child_hash;
        
        /* Record offset before parsing element */
        if (count >= capacity)
        {
            capacity *= 2;
            sentries = repalloc(sentries, sizeof(SEntry) * capacity);
            child_hashes = repalloc(child_hashes, sizeof(uint32) * capacity);
        }
        elem_start = elements.len;
        
        /* Parse element and get its hash */
        child_hash = sexp_parse_value_with_hash(state);
        child_hashes[count] = child_hash;
        
        /* Get type from first byte of parsed element */
        first_byte = (uint8)elements.data[elem_start];
        sentry_type = get_sentry_type_from_tag(first_byte);
        
        /* Create SEntry: type + offset */
        sentries[count] = SENTRY_MAKE(sentry_type, elem_start);
        
        count++;
        skip_whitespace(state);
    }
    
    state->output = saved_output;
    
    if (state->ptr >= state->end)
    {
        pfree(elements.data);
        pfree(sentries);
        pfree(child_hashes);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("unterminated list")));
    }
    
    /* Skip closing paren */
    Assert(*state->ptr == ')');
    state->ptr++;
    
    state->depth--;
    
    /* Compute list structural hash by combining all child hashes */
    list_hash = sexp_hash_uint32((uint32)count);  /* Start with count hash */
    list_hash = hash_combine(list_hash, sexp_hash_uint32(SEXP_TAG_LIST));  /* Include type */
    for (i = 0; i < count; i++)
    {
        list_hash = sexp_hash_combine(list_hash, child_hashes[i], i);
    }
    
    /*
     * Write list with SEntry table for O(1) access with no indirection.
     * 
     * Format:
     * - Small lists (count <= SEXP_SMALL_LIST_MAX) include size prefix for O(1) skipping
     * - Large lists include SEntry table and structural hash for O(1) random access
     * 
     * Small list format: [tag|count][varint:total_payload_size][elements...]
     * Large list format: [tag][uint32:count][uint32:hash][SEntry table][elements...]
     */
    if (count <= SEXP_SMALL_LIST_MAX)
    {
        /* Small list v6: tag with inline count + payload size for O(1) skip */
        appendStringInfoChar(state->output, SEXP_TAG_LIST | (uint8)count);
        /* Write payload size as varint - enables skip_element in O(1) */
        write_varint(state->output, (uint64)elements.len);
        appendBinaryStringInfo(state->output, elements.data, elements.len);
    }
    else
    {
        uint32 cnt32 = (uint32)count;
        
        /* Large list: tag + count + hash + SEntry table + elements */
        appendStringInfoChar(state->output, SEXP_TAG_LIST);
        
        /* Write count as uint32 */
        appendBinaryStringInfo(state->output, (char *)&cnt32, sizeof(uint32));
        
        /* Write structural hash as uint32 */
        appendBinaryStringInfo(state->output, (char *)&list_hash, sizeof(uint32));
        
        /* Write SEntry table */
        appendBinaryStringInfo(state->output, (char *)sentries, count * sizeof(SEntry));
        
        /* Write element data */
        appendBinaryStringInfo(state->output, elements.data, elements.len);
    }
    
    pfree(elements.data);
    pfree(sentries);
    pfree(child_hashes);
    
    return list_hash;
}

/*
 * parse_atom - Parse a non-list atom (symbol or number)
 * Returns: structural hash of the atom
 */
static uint32
parse_atom(SexpParseState *state)
{
    if (*state->ptr == '"')
    {
        return parse_string(state);
    }
    else
    {
        return parse_number_or_symbol(state);
    }
}

/*
 * parse_string - Parse a quoted string: "..."
 * Returns: structural hash of the string content
 */
static uint32
parse_string(SexpParseState *state)
{
    StringInfoData str;
    uint32 hash;
    
    Assert(*state->ptr == '"');
    state->ptr++;
    
    initStringInfo(&str);
    
    while (state->ptr < state->end && *state->ptr != '"')
    {
        if (*state->ptr == '\\')
        {
            state->ptr++;
            if (state->ptr >= state->end)
            {
                pfree(str.data);
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                         errmsg("unterminated string escape")));
            }
            
            switch (*state->ptr)
            {
                case 'n':
                    appendStringInfoChar(&str, '\n');
                    break;
                case 't':
                    appendStringInfoChar(&str, '\t');
                    break;
                case 'r':
                    appendStringInfoChar(&str, '\r');
                    break;
                case '\\':
                    appendStringInfoChar(&str, '\\');
                    break;
                case '"':
                    appendStringInfoChar(&str, '"');
                    break;
                default:
                    appendStringInfoChar(&str, *state->ptr);
                    break;
            }
        }
        else
        {
            appendStringInfoChar(&str, *state->ptr);
        }
        state->ptr++;
    }
    
    if (state->ptr >= state->end)
    {
        pfree(str.data);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("unterminated string")));
    }
    
    /* Skip closing quote */
    Assert(*state->ptr == '"');
    state->ptr++;
    
    /* Compute hash of string content using PostgreSQL hash functions */
    hash = sexp_hash_string_with_tag(SEXP_TAG_SHORT_STRING, str.data, str.len);
    
    /* Write string using compact encoding */
    if (str.len <= SEXP_SHORT_STRING_MAX)
    {
        /* Short string: tag includes length */
        appendStringInfoChar(state->output, SEXP_TAG_SHORT_STRING | (uint8)str.len);
        appendBinaryStringInfo(state->output, str.data, str.len);
    }
    else
    {
        /* Long string: tag + varint length + data */
        appendStringInfoChar(state->output, SEXP_TAG_LONG_STRING);
        write_varint(state->output, str.len);
        appendBinaryStringInfo(state->output, str.data, str.len);
    }
    
    pfree(str.data);
    return hash;
}

/*
 * parse_number_or_symbol - Parse number or symbol
 * Returns: structural hash of the parsed value
 */
static uint32
parse_number_or_symbol(SexpParseState *state)
{
    const char *start = state->ptr;
    const char *end;
    bool is_number = true;
    bool has_dot = false;
    bool has_digit = false;
    char c;
    uint32 hash;
    
    /* Scan the token */
    while (state->ptr < state->end && 
           !isspace((unsigned char) *state->ptr) &&
           *state->ptr != '(' && 
           *state->ptr != ')' &&
           *state->ptr != '"' &&
           *state->ptr != ';')
    {
        c = *state->ptr;
        
        if (c == '-' || c == '+')
        {
            if (state->ptr != start)
                is_number = false;
        }
        else if (c == '.')
        {
            if (has_dot)
                is_number = false;
            has_dot = true;
        }
        else if (isdigit((unsigned char) c))
        {
            has_digit = true;
        }
        else
        {
            is_number = false;
        }
        
        state->ptr++;
    }
    
    end = state->ptr;
    
    if (start == end)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("empty atom")));
    }
    
    /* Check for nil */
    if (end - start == 3 && strncmp(start, "nil", 3) == 0)
    {
        appendStringInfoChar(state->output, SEXP_TAG_NIL);
        return 0;  /* NIL has hash 0 */
    }
    
    /* Determine if it's a number */
    is_number = is_number && has_digit;
    
    if (is_number)
    {
        char *numstr = pnstrdup(start, end - start);
        char *endptr;
        
        if (has_dot)
        {
            /* Float */
            float8 val = strtod(numstr, &endptr);
            uint32 type_hash, value_hash;
            if (*endptr != '\0')
            {
                pfree(numstr);
                goto parse_symbol;
            }
            appendStringInfoChar(state->output, SEXP_TAG_FLOAT);
            appendBinaryStringInfo(state->output, (char *)&val, sizeof(float8));
            
            /* 
             * Semantic hash: type tag + value hash.
             * MUST match sexp_element_hash() formula exactly.
             */
            type_hash = sexp_hash_uint32(SEXP_TAG_FLOAT);
            value_hash = sexp_hash_float64(val);
            hash = hash_combine(type_hash, value_hash);
            
            pfree(numstr);
            return hash;
        }
        else
        {
            /* Integer */
            int64 val = strtoll(numstr, &endptr, 10);
            uint32 type_hash, value_hash;
            if (*endptr != '\0')
            {
                pfree(numstr);
                goto parse_symbol;
            }
            
            /* Use small int encoding if possible */
            if (val >= SEXP_SMALLINT_MIN && val <= SEXP_SMALLINT_MAX)
            {
                uint8 encoded = SEXP_TAG_SMALLINT | (uint8)((int)val + SEXP_SMALLINT_BIAS);
                appendStringInfoChar(state->output, encoded);
            }
            else
            {
                /* Full integer with zigzag encoding */
                appendStringInfoChar(state->output, SEXP_TAG_INTEGER);
                write_signed_varint(state->output, val);
            }
            
            /* 
             * Semantic hash: type tag + value hash.
             * Use SEXP_TAG_INTEGER for both smallint and integer (same semantic type).
             * MUST match sexp_element_hash() formula exactly.
             */
            type_hash = sexp_hash_uint32(SEXP_TAG_INTEGER);
            value_hash = sexp_hash_int64(val);
            hash = hash_combine(type_hash, value_hash);
            
            pfree(numstr);
            return hash;
        }
    }
    
parse_symbol:
    {
        int sym_idx = intern_symbol(state, start, end - start);
        appendStringInfoChar(state->output, SEXP_TAG_SYMBOL_REF);
        write_varint(state->output, sym_idx);
        
        /* Hash symbol by its string content (not index, for cross-sexp comparison) */
        hash = sexp_hash_string_with_tag(SEXP_TAG_SYMBOL_REF, start, end - start);
        return hash;
    }
}
