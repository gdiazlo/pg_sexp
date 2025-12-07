/*
 * sexp_match.c
 *
 * Pattern matching for S-expressions
 *
 * Pattern syntax:
 *   _       - Match any single element (wildcard)
 *   _*      - Match zero or more elements (rest/spread)
 *   ?name   - Capture single element as 'name'
 *   ??name  - Capture rest of elements as 'name'
 *   literal - Match exactly
 */

#include "pg_sexp.h"
#include "sexp_debug.h"
#include <string.h>

/* Forward declarations */
static bool match_elements(SexpReadState *expr_state, SexpReadState *pat_state,
                          SexpMatchResult *result);
static bool is_pattern_symbol(const char *sym, int len, PatternType *type, 
                             char *capture_name);
static void skip_element_internal(SexpReadState *state);
static bool elements_match(SexpReadState *expr_state, SexpReadState *pat_state,
                          SexpMatchResult *result);

/*
 * Check if a symbol is a pattern special symbol
 * Returns true if it's a pattern symbol, sets type and capture_name
 */
static bool
is_pattern_symbol(const char *sym, int len, PatternType *type, char *capture_name)
{
    if (len == 1 && sym[0] == '_')
    {
        *type = PAT_WILDCARD;
        capture_name[0] = '\0';
        return true;
    }
    
    if (len == 2 && sym[0] == '_' && sym[1] == '*')
    {
        *type = PAT_WILDCARD_REST;
        capture_name[0] = '\0';
        return true;
    }
    
    if (len >= 2 && sym[0] == '?' && sym[1] == '?')
    {
        /* ??name - capture rest */
        *type = PAT_CAPTURE_REST;
        if (len > 2 && len < 66)
        {
            memcpy(capture_name, sym + 2, len - 2);
            capture_name[len - 2] = '\0';
        }
        else
        {
            capture_name[0] = '\0';
        }
        return true;
    }
    
    if (len >= 1 && sym[0] == '?')
    {
        /* ?name - capture single */
        *type = PAT_CAPTURE;
        if (len > 1 && len < 65)
        {
            memcpy(capture_name, sym + 1, len - 1);
            capture_name[len - 1] = '\0';
        }
        else
        {
            capture_name[0] = '\0';
        }
        return true;
    }
    
    return false;
}

/*
 * Skip element - local version to avoid dependency issues
 */
static void
skip_element_internal(SexpReadState *state)
{
    uint8 byte;
    uint8 tag;
    uint64 count;
    uint64 len;
    uint64 i;
    
    if (state->ptr >= state->end)
        return;
    
    byte = *state->ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    switch (tag)
    {
        case SEXP_TAG_NIL:
        case SEXP_TAG_SMALLINT:
            break;
            
        case SEXP_TAG_INTEGER:
            (void)decode_varint(&state->ptr, state->end);
            break;
            
        case SEXP_TAG_FLOAT:
            state->ptr += sizeof(float8);
            break;
            
        case SEXP_TAG_SYMBOL_REF:
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
            count = byte & SEXP_DATA_MASK;
            if (unlikely(count == 0))
            {
                uint32 cnt32;
                cnt32 = SEXP_LOAD_UINT32_UNALIGNED(state->ptr);
                state->ptr += sizeof(uint32);
                count = cnt32;
                /* Skip structural hash (v6) */
                state->ptr += sizeof(uint32);
                /* Skip SEntry table */
                state->ptr += count * sizeof(SEntry);
            }
            else
            {
                /* Small list v6: skip size prefix (varint) */
                (void)decode_varint(&state->ptr, state->end);
            }
            for (i = 0; i < count; i++)
                skip_element_internal(state);
            break;
    }
}

/*
 * Extract element at current position as a new Sexp
 */
static Sexp *
extract_current_element(SexpReadState *state, Sexp *parent)
{
    uint8 *elem_start = state->ptr;
    uint8 *elem_end;
    Sexp *result;
    int header_size;
    int elem_size;
    int total_size;
    uint8 *parent_data;
    uint8 *ptr;
    uint64 sym_count;
    int i;
    
    /* Measure element size */
    skip_element_internal(state);
    elem_end = state->ptr;
    
    /* Reset pointer */
    state->ptr = elem_start;
    skip_element_internal(state);
    
    /* Calculate header size from parent */
    parent_data = (uint8 *)&parent->version;
    ptr = parent_data + 1;
    sym_count = decode_varint(&ptr, parent_data + VARSIZE(parent) - VARHDRSZ);
    for (i = 0; i < (int)sym_count; i++)
    {
        uint64 slen = decode_varint(&ptr, parent_data + VARSIZE(parent) - VARHDRSZ);
        ptr += slen;
    }
    header_size = (int)(ptr - parent_data);
    elem_size = (int)(elem_end - elem_start);
    
    /* Allocate result */
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
 * Add a capture to the result
 * Currently unused but kept for potential capture extraction feature.
 */
#ifdef SEXP_CAPTURE_SUPPORT
static bool
add_capture(SexpMatchResult *result, const char *name, Sexp *value, bool is_rest)
{
    SexpCapture *cap;
    
    if (result->capture_count >= SEXP_MAX_CAPTURES)
        return false;
    
    cap = &result->captures[result->capture_count++];
    strncpy(cap->name, name, sizeof(cap->name) - 1);
    cap->name[sizeof(cap->name) - 1] = '\0';
    cap->value = value;
    cap->is_rest = is_rest;
    cap->rest_count = 0;
    cap->rest_values = NULL;
    
    return true;
}
#endif

/*
 * Check if two elements match, handling pattern symbols
 */
static bool
elements_match(SexpReadState *expr_state, SexpReadState *pat_state,
               SexpMatchResult *result)
{
    uint8 expr_byte, pat_byte;
    uint8 expr_tag, pat_tag;
    
    if (expr_state->ptr >= expr_state->end || pat_state->ptr >= pat_state->end)
        return (expr_state->ptr >= expr_state->end) && (pat_state->ptr >= pat_state->end);
    
    pat_byte = *pat_state->ptr;
    pat_tag = pat_byte & SEXP_TAG_MASK;
    
    /* Check if pattern element is a special symbol */
    if (pat_tag == SEXP_TAG_SYMBOL_REF)
    {
        uint8 *saved_ptr = pat_state->ptr;
        PatternType ptype;
        char capture_name[64];
        uint64 sym_idx;
        
        pat_state->ptr++;
        sym_idx = decode_varint(&pat_state->ptr, pat_state->end);
        
        if ((int)sym_idx < pat_state->sym_count)
        {
            const char *sym = pat_state->symbols[sym_idx];
            int sym_len = pat_state->sym_lengths[sym_idx];
            
            if (is_pattern_symbol(sym, sym_len, &ptype, capture_name))
            {
                switch (ptype)
                {
                    case PAT_WILDCARD:
                        /* Match any single element */
                        skip_element_internal(expr_state);
                        return true;
                        
                    case PAT_CAPTURE:
                        /* Capture single element */
                        if (result && capture_name[0])
                        {
                            /* Need parent Sexp to extract - for now, skip */
                            /* TODO: implement proper capture extraction */
                        }
                        skip_element_internal(expr_state);
                        return true;
                        
                    case PAT_WILDCARD_REST:
                    case PAT_CAPTURE_REST:
                        /* These should be handled at the list level */
                        pat_state->ptr = saved_ptr;
                        return false;
                        
                    default:
                        break;
                }
            }
        }
        
        /* Not a pattern symbol, restore and continue with normal matching */
        pat_state->ptr = saved_ptr;
    }
    
    /* Normal element matching */
    expr_byte = *expr_state->ptr++;
    pat_byte = *pat_state->ptr++;
    expr_tag = expr_byte & SEXP_TAG_MASK;
    pat_tag = pat_byte & SEXP_TAG_MASK;
    
    if (expr_tag != pat_tag)
        return false;
    
    switch (expr_tag)
    {
        case SEXP_TAG_NIL:
            return true;
            
        case SEXP_TAG_SMALLINT:
            return expr_byte == pat_byte;
            
        case SEXP_TAG_INTEGER:
        {
            uint64 expr_val = decode_varint(&expr_state->ptr, expr_state->end);
            uint64 pat_val = decode_varint(&pat_state->ptr, pat_state->end);
            return expr_val == pat_val;
        }
        
        case SEXP_TAG_FLOAT:
        {
            float8 expr_val, pat_val;
            memcpy(&expr_val, expr_state->ptr, sizeof(float8));
            memcpy(&pat_val, pat_state->ptr, sizeof(float8));
            expr_state->ptr += sizeof(float8);
            pat_state->ptr += sizeof(float8);
            return expr_val == pat_val;
        }
        
        case SEXP_TAG_SYMBOL_REF:
        {
            uint64 expr_idx = decode_varint(&expr_state->ptr, expr_state->end);
            uint64 pat_idx = decode_varint(&pat_state->ptr, pat_state->end);
            
            if ((int)expr_idx >= expr_state->sym_count || 
                (int)pat_idx >= pat_state->sym_count)
                return false;
            
            if (expr_state->sym_lengths[expr_idx] != pat_state->sym_lengths[pat_idx])
                return false;
            
            return memcmp(expr_state->symbols[expr_idx], 
                         pat_state->symbols[pat_idx],
                         expr_state->sym_lengths[expr_idx]) == 0;
        }
        
        case SEXP_TAG_SHORT_STRING:
        {
            int expr_len = expr_byte & SEXP_DATA_MASK;
            int pat_len = pat_byte & SEXP_DATA_MASK;
            bool match;
            
            if (expr_len != pat_len)
            {
                expr_state->ptr += expr_len;
                pat_state->ptr += pat_len;
                return false;
            }
            
            match = (memcmp(expr_state->ptr, pat_state->ptr, expr_len) == 0);
            expr_state->ptr += expr_len;
            pat_state->ptr += pat_len;
            return match;
        }
        
        case SEXP_TAG_LONG_STRING:
        {
            uint64 expr_len = decode_varint(&expr_state->ptr, expr_state->end);
            uint64 pat_len = decode_varint(&pat_state->ptr, pat_state->end);
            bool match;
            
            if (expr_len != pat_len)
            {
                expr_state->ptr += expr_len;
                pat_state->ptr += pat_len;
                return false;
            }
            
            match = (memcmp(expr_state->ptr, pat_state->ptr, expr_len) == 0);
            expr_state->ptr += expr_len;
            pat_state->ptr += pat_len;
            return match;
        }
        
        case SEXP_TAG_LIST:
            return match_elements(expr_state, pat_state, result);
            
        default:
            return false;
    }
}

/*
 * Match list elements, handling _* and ??rest patterns
 */
static bool
match_elements(SexpReadState *expr_state, SexpReadState *pat_state,
               SexpMatchResult *result)
{
    uint64 expr_count, pat_count;
    uint64 expr_i, pat_i;
    uint8 expr_byte, pat_byte;
    
    /* Decode expression list header (we already consumed the tag byte) */
    expr_byte = *(expr_state->ptr - 1);
    expr_count = expr_byte & SEXP_DATA_MASK;
    if (unlikely(expr_count == 0))
    {
        uint32 cnt32;
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(expr_state->ptr);
        expr_state->ptr += sizeof(uint32);
        expr_count = cnt32;
        /* Skip hash (v6) */
        expr_state->ptr += sizeof(uint32);
        /* Skip SEntry table */
        expr_state->ptr += expr_count * sizeof(SEntry);
    }
    else
    {
        /* Small list v6: skip size prefix */
        (void)decode_varint(&expr_state->ptr, expr_state->end);
    }
    
    /* Decode pattern list header */
    pat_byte = *(pat_state->ptr - 1);
    pat_count = pat_byte & SEXP_DATA_MASK;
    if (unlikely(pat_count == 0))
    {
        uint32 cnt32;
        cnt32 = SEXP_LOAD_UINT32_UNALIGNED(pat_state->ptr);
        pat_state->ptr += sizeof(uint32);
        pat_count = cnt32;
        /* Skip hash (v6) */
        pat_state->ptr += sizeof(uint32);
        /* Skip SEntry table */
        pat_state->ptr += pat_count * sizeof(SEntry);
    }
    else
    {
        /* Small list v6: skip size prefix */
        (void)decode_varint(&pat_state->ptr, pat_state->end);
    }
    
    expr_i = 0;
    pat_i = 0;
    
    while (pat_i < pat_count)
    {
        uint8 pat_elem_byte;
        uint8 pat_elem_tag;
        
        /* Check if current pattern element is a rest pattern (_* or ??name) */
        pat_elem_byte = *pat_state->ptr;
        pat_elem_tag = pat_elem_byte & SEXP_TAG_MASK;
        
        if (pat_elem_tag == SEXP_TAG_SYMBOL_REF)
        {
            PatternType ptype;
            char capture_name[64];
            uint64 sym_idx;
            uint8 *check_ptr = pat_state->ptr + 1;
            
            sym_idx = decode_varint(&check_ptr, pat_state->end);
            
            if ((int)sym_idx < pat_state->sym_count)
            {
                const char *sym = pat_state->symbols[sym_idx];
                int sym_len = pat_state->sym_lengths[sym_idx];
                
                if (is_pattern_symbol(sym, sym_len, &ptype, capture_name))
                {
                    if (ptype == PAT_WILDCARD_REST || ptype == PAT_CAPTURE_REST)
                    {
                        /* Rest pattern - must be last in pattern list */
                        if (pat_i + 1 != pat_count)
                        {
                            /* Not last element - this is an error in the pattern */
                            return false;
                        }
                        
                        /* Match! Consume all remaining expression elements */
                        while (expr_i < expr_count)
                        {
                            skip_element_internal(expr_state);
                            expr_i++;
                        }
                        
                        /* Skip the rest pattern element */
                        pat_state->ptr = check_ptr;
                        pat_i++;
                        
                        /* All matched */
                        return true;
                    }
                }
            }
        }
        
        /* Not a rest pattern - need exactly one expression element */
        if (expr_i >= expr_count)
            return false;
        
        /* Match this element */
        if (!elements_match(expr_state, pat_state, result))
            return false;
        
        expr_i++;
        pat_i++;
    }
    
    /* All pattern elements matched - check if expression has leftover elements */
    return (expr_i == expr_count);
}

/*
 * sexp_match - Check if expression matches pattern
 */
bool
sexp_match(Sexp *expr, Sexp *pattern)
{
    SexpReadState expr_state;
    SexpReadState pat_state;
    bool result;
    
    sexp_init_read_state(&expr_state, expr);
    sexp_init_read_state(&pat_state, pattern);
    
    result = elements_match(&expr_state, &pat_state, NULL);
    
    sexp_free_read_state(&expr_state);
    sexp_free_read_state(&pat_state);
    
    return result;
}

/*
 * sexp_match_with_captures - Match with capture extraction
 */
bool
sexp_match_with_captures(Sexp *expr, Sexp *pattern, SexpMatchResult *result)
{
    SexpReadState expr_state;
    SexpReadState pat_state;
    bool matched;
    
    if (result)
    {
        result->matched = false;
        result->capture_count = 0;
    }
    
    sexp_init_read_state(&expr_state, expr);
    sexp_init_read_state(&pat_state, pattern);
    
    matched = elements_match(&expr_state, &pat_state, result);
    
    if (result)
        result->matched = matched;
    
    sexp_free_read_state(&expr_state);
    sexp_free_read_state(&pat_state);
    
    return matched;
}

/*
 * Recursive search for pattern in expression
 * 
 * OPTIMIZATION: Takes a pre-initialized pattern state to avoid re-parsing
 * the pattern's symbol table for every element visited. The pattern state's
 * ptr is reset to pat_start before each match attempt.
 */
static bool
find_pattern_recursive(SexpReadState *expr_state, Sexp *expr_parent,
                       SexpReadState *pat_state, uint8 *pat_start, Sexp **found)
{
    uint8 byte;
    uint8 tag;
    uint8 *start_ptr;
    
    if (expr_state->ptr >= expr_state->end)
        return false;
    
    start_ptr = expr_state->ptr;
    
    /* Try matching at current position */
    /* Reset pattern state pointer to start of pattern element */
    pat_state->ptr = pat_start;
    
    /* Save expression state for potential match */
    {
        SexpReadState expr_copy;
        expr_copy = *expr_state;
        expr_copy.ptr = start_ptr;
        
        if (elements_match(&expr_copy, pat_state, NULL))
        {
            /* Match found! Extract this element */
            *found = extract_current_element(expr_state, expr_parent);
            return true;
        }
    }
    
    /* No match at this position - if it's a list, search children */
    byte = *expr_state->ptr++;
    tag = byte & SEXP_TAG_MASK;
    
    if (tag == SEXP_TAG_LIST)
    {
        uint64 count;
        uint64 i;
        
        count = byte & SEXP_DATA_MASK;
        if (unlikely(count == 0))
        {
            uint32 cnt32;
            cnt32 = SEXP_LOAD_UINT32_UNALIGNED(expr_state->ptr);
            expr_state->ptr += sizeof(uint32);
            count = cnt32;
            /* Skip hash (v6) */
            expr_state->ptr += sizeof(uint32);
            /* Skip SEntry table */
            expr_state->ptr += count * sizeof(SEntry);
        }
        else
        {
            /* Small list v6: skip size prefix */
            (void)decode_varint(&expr_state->ptr, expr_state->end);
        }
        
        for (i = 0; i < count; i++)
        {
            if (find_pattern_recursive(expr_state, expr_parent, pat_state, pat_start, found))
                return true;
        }
    }
    else
    {
        /* Skip this non-list element */
        expr_state->ptr = start_ptr;
        skip_element_internal(expr_state);
    }
    
    return false;
}

/*
 * sexp_find_first - Find first subexpression matching pattern
 * 
 * OPTIMIZATION: Initializes pattern state once and passes it to recursive
 * search, avoiding O(N) re-parsing of the pattern's symbol table where N
 * is the number of elements visited.
 */
Sexp *
sexp_find_first(Sexp *expr, Sexp *pattern)
{
    SexpReadState expr_state;
    SexpReadState pat_state;
    Sexp *found = NULL;
    uint8 *pat_start;
    
    sexp_init_read_state(&expr_state, expr);
    sexp_init_read_state(&pat_state, pattern);
    
    /* Save the starting position of the pattern element */
    pat_start = pat_state.ptr;
    
    find_pattern_recursive(&expr_state, expr, &pat_state, pat_start, &found);
    
    sexp_free_read_state(&expr_state);
    sexp_free_read_state(&pat_state);
    
    return found;
}
