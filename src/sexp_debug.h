/*
 * sexp_debug.h
 *
 * Defensive programming macros for pg_sexp
 *
 * Design philosophy for fil-c compatibility:
 * ==========================================
 * When compiled with fil-c, memory safety (bounds checking, use-after-free, etc.)
 * is enforced by the compiler. Therefore:
 *
 *   1. SEXP_DEBUG_* macros: Debug-only checks, compiled out in release builds.
 *      These are for internal invariants that fil-c will catch anyway.
 *
 *   2. SEXP_CHECK_* macros: Always-on checks for USER-FACING validation only.
 *      These report meaningful errors for malformed input (not memory safety).
 *      Examples: version mismatch, depth exceeded, invalid format.
 *
 *   3. PostgreSQL Assert(): Use for internal invariants in debug builds.
 *
 * The goal is ZERO runtime overhead in release builds for checks that
 * fil-c handles, while keeping user-friendly error messages for input validation.
 */

#ifndef SEXP_DEBUG_H
#define SEXP_DEBUG_H

#include "postgres.h"

/*
 * =============================================================================
 * DEBUG-ONLY MACROS (compiled out in release builds)
 * These are redundant when using fil-c but helpful for development/debugging.
 * =============================================================================
 */

#ifdef USE_ASSERT_CHECKING

#define SEXP_DEBUG_PTR_BOUNDS(ptr, start, end, msg) \
    do { \
        if ((uint8 *)(ptr) < (uint8 *)(start) || (uint8 *)(ptr) > (uint8 *)(end)) \
            elog(PANIC, "sexp bounds violation: %s (ptr=%p, range=[%p,%p]) at %s:%d", \
                 (msg), (void *)(ptr), (void *)(start), (void *)(end), __FILE__, __LINE__); \
    } while (0)

#define SEXP_DEBUG_READ_BOUNDS(ptr, nbytes, end, msg) \
    do { \
        if ((uint8 *)(ptr) + (nbytes) > (uint8 *)(end)) \
            elog(PANIC, "sexp read overflow: %s (need %d bytes at %p, end=%p) at %s:%d", \
                 (msg), (int)(nbytes), (void *)(ptr), (void *)(end), __FILE__, __LINE__); \
    } while (0)

#define SEXP_DEBUG_SYMBOL_INDEX(idx, count, msg) \
    do { \
        if ((uint64)(idx) >= (uint64)(count)) \
            elog(PANIC, "sexp symbol index OOB: %s (idx=%llu, count=%d) at %s:%d", \
                 (msg), (unsigned long long)(idx), (int)(count), __FILE__, __LINE__); \
    } while (0)

#define SEXP_DEBUG_NOT_NULL(ptr, msg) \
    do { \
        if ((ptr) == NULL) \
            elog(PANIC, "sexp null pointer: %s at %s:%d", (msg), __FILE__, __LINE__); \
    } while (0)

#define SEXP_ASSERT_INVARIANT(cond, msg) \
    do { \
        if (!(cond)) \
            elog(PANIC, "sexp invariant violation: %s at %s:%d", (msg), __FILE__, __LINE__); \
    } while (0)

#else /* !USE_ASSERT_CHECKING */

/* All debug macros compile to nothing in release builds */
#define SEXP_DEBUG_PTR_BOUNDS(ptr, start, end, msg)     ((void) 0)
#define SEXP_DEBUG_READ_BOUNDS(ptr, nbytes, end, msg)   ((void) 0)
#define SEXP_DEBUG_SYMBOL_INDEX(idx, count, msg)        ((void) 0)
#define SEXP_DEBUG_NOT_NULL(ptr, msg)                   ((void) 0)
#define SEXP_ASSERT_INVARIANT(cond, msg)                ((void) 0)

#endif /* USE_ASSERT_CHECKING */


/*
 * =============================================================================
 * USER-FACING VALIDATION MACROS (always active)
 * These provide meaningful error messages for malformed input.
 * They do NOT check memory safety (fil-c handles that).
 * =============================================================================
 */

/*
 * SEXP_CHECK_VERSION - Validate format version
 * User needs to know if their data was created with a newer version.
 */
#define SEXP_CHECK_VERSION(version) \
    do { \
        if ((version) > SEXP_FORMAT_VERSION) \
            ereport(ERROR, \
                    (errcode(ERRCODE_DATA_CORRUPTED), \
                     errmsg("sexp format version %d not supported (max %d)", \
                            (int)(version), SEXP_FORMAT_VERSION))); \
    } while (0)

/*
 * SEXP_CHECK_DEPTH - Validate nesting depth during parsing
 * Prevents stack overflow from malicious/malformed deeply nested input.
 */
#define SEXP_CHECK_DEPTH(depth) \
    do { \
        if ((depth) >= SEXP_MAX_DEPTH) \
            ereport(ERROR, \
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), \
                     errmsg("sexp nesting depth exceeded: %d (max %d)", \
                            (int)(depth), SEXP_MAX_DEPTH))); \
    } while (0)

/*
 * SEXP_CHECK_TAG - Validate tag byte (user-facing error for corrupted data)
 */
#define SEXP_CHECK_TAG(tag) \
    do { \
        uint8 _tag = (tag) & SEXP_TAG_MASK; \
        if (_tag != SEXP_TAG_NIL && \
            _tag != SEXP_TAG_SMALLINT && \
            _tag != SEXP_TAG_INTEGER && \
            _tag != SEXP_TAG_FLOAT && \
            _tag != SEXP_TAG_SYMBOL_REF && \
            _tag != SEXP_TAG_SHORT_STRING && \
            _tag != SEXP_TAG_LONG_STRING && \
            _tag != SEXP_TAG_LIST) \
            ereport(ERROR, \
                    (errcode(ERRCODE_DATA_CORRUPTED), \
                     errmsg("sexp invalid tag byte: 0x%02x", (int)(tag)))); \
    } while (0)

/*
 * =============================================================================
 * LEGACY COMPATIBILITY MACROS
 * These map old names to new debug-only versions for backward compatibility.
 * =============================================================================
 */

#define SEXP_CHECK_PTR_BOUNDS(ptr, start, end, msg)     SEXP_DEBUG_PTR_BOUNDS(ptr, start, end, msg)
#define SEXP_CHECK_READ_BOUNDS(ptr, nbytes, end, msg)   SEXP_DEBUG_READ_BOUNDS(ptr, nbytes, end, msg)
#define SEXP_CHECK_SYMBOL_INDEX(idx, count, msg)        SEXP_DEBUG_SYMBOL_INDEX(idx, count, msg)
#define SEXP_CHECK_NOT_NULL(ptr, msg)                   SEXP_DEBUG_NOT_NULL(ptr, msg)

/* These were always runtime - keep for now but may want to make debug-only */
#define SEXP_CHECK_LIST_COUNT(count, max_reasonable) \
    SEXP_ASSERT_INVARIANT((uint32)(count) <= (uint32)(max_reasonable), "list count unreasonable")

#define SEXP_CHECK_VARINT_OVERFLOW(shift) \
    SEXP_ASSERT_INVARIANT((shift) < 64, "varint overflow")


/*
 * =============================================================================
 * REASONABLE LIMITS
 * Used for sanity checks that prevent obvious corruption/DoS.
 * =============================================================================
 */

#define SEXP_MAX_REASONABLE_LIST_COUNT  (1024 * 1024)       /* 1M elements */
#define SEXP_MAX_REASONABLE_STRING_LEN  (100 * 1024 * 1024) /* 100MB */
#define SEXP_MAX_REASONABLE_DATA_SIZE   (1024 * 1024 * 1024) /* 1GB */

#endif /* SEXP_DEBUG_H */
