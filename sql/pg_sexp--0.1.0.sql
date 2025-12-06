-- pg_sexp extension SQL definitions
-- S-expression data type for PostgreSQL

-- Create the sexp data type
CREATE TYPE sexp;

-- Input/Output functions
CREATE FUNCTION sexp_in(cstring)
    RETURNS sexp
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sexp_out(sexp)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sexp_recv(internal)
    RETURNS sexp
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sexp_send(sexp)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Complete the type definition
CREATE TYPE sexp (
    INPUT = sexp_in,
    OUTPUT = sexp_out,
    RECEIVE = sexp_recv,
    SEND = sexp_send,
    STORAGE = extended
);

-- Cast from text to sexp
CREATE CAST (text AS sexp)
    WITH INOUT
    AS IMPLICIT;

-- Cast from sexp to text
CREATE CAST (sexp AS text)
    WITH INOUT;

-- Equality operators
CREATE FUNCTION sexp_eq(sexp, sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION sexp_ne(sexp, sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR = (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_eq,
    COMMUTATOR = =,
    NEGATOR = <>,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    HASHES,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_ne,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);

-- Hash support for hash indexes and hash joins
CREATE FUNCTION sexp_hash(sexp)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Extended hash with seed for parallel operations (PostgreSQL 11+)
CREATE FUNCTION sexp_hash_extended(sexp, bigint)
    RETURNS bigint
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS sexp_ops
    DEFAULT FOR TYPE sexp USING hash AS
    OPERATOR 1 = (sexp, sexp),
    FUNCTION 1 sexp_hash(sexp),
    FUNCTION 2 sexp_hash_extended(sexp, bigint);

-- List operations

-- car - get first element of a list
CREATE FUNCTION car(sexp)
    RETURNS sexp
    AS 'MODULE_PATHNAME', 'sexp_car_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- cdr - get rest of list (tail)
CREATE FUNCTION cdr(sexp)
    RETURNS sexp
    AS 'MODULE_PATHNAME', 'sexp_cdr_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- length - get number of elements
CREATE FUNCTION sexp_length(sexp)
    RETURNS integer
    AS 'MODULE_PATHNAME', 'sexp_length_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- nth - get nth element (0-indexed)
CREATE FUNCTION nth(sexp, integer)
    RETURNS sexp
    AS 'MODULE_PATHNAME', 'sexp_nth_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- head - alias for car
CREATE FUNCTION head(sexp)
    RETURNS sexp
    AS 'MODULE_PATHNAME', 'sexp_head_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Type inspection functions

-- typeof - get type name
CREATE FUNCTION sexp_typeof(sexp)
    RETURNS text
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- is_nil - check if nil/empty list
CREATE FUNCTION is_nil(sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_is_nil'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- is_list - check if list (including nil)
CREATE FUNCTION is_list(sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_is_list'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- is_atom - check if atom (symbol, string, or number)
CREATE FUNCTION is_atom(sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_is_atom'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- is_symbol - check if symbol
CREATE FUNCTION is_symbol(sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_is_symbol'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- is_string - check if string
CREATE FUNCTION is_string(sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_is_string'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- is_number - check if number (integer or float)
CREATE FUNCTION is_number(sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_is_number'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Containment operators
--
-- @>  : Structural containment (exact subtree match)
-- @>> : Key-based containment (list head as key, order-independent)

-- Structural containment (@>)
CREATE FUNCTION sexp_contains(sexp, sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_contains_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- @> operator for structural containment
CREATE OPERATOR @> (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_contains,
    COMMUTATOR = <@,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- <@ operator for contained by (structural)
CREATE FUNCTION sexp_contained(sexp, sexp)
    RETURNS boolean
    AS 'SELECT $2 @> $1'
    LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <@ (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_contained,
    COMMUTATOR = @>,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- Key-based containment (@>>)
-- Treats list heads as "keys", matches elements in any order (like jsonb @>)
CREATE FUNCTION sexp_contains_key(sexp, sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_contains_key_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- @>> operator for key-based containment
CREATE OPERATOR @>> (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_contains_key,
    COMMUTATOR = <<@,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- <<@ operator for key-based contained by
CREATE FUNCTION sexp_contained_key(sexp, sexp)
    RETURNS boolean
    AS 'SELECT $2 @>> $1'
    LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR <<@ (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_contained_key,
    COMMUTATOR = @>>,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- Pattern matching functions
--
-- Pattern syntax:
--   _       - Match any single element (wildcard)
--   _*      - Match zero or more elements (rest/spread)
--   ?name   - Capture single element (name is ignored, for readability)
--   ??name  - Capture rest of elements (name is ignored, for readability)
--   literal - Match exactly

-- sexp_match - Check if expression matches pattern
CREATE FUNCTION sexp_match(sexp, sexp)
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'sexp_match_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- @~ operator for pattern matching
CREATE OPERATOR @~ (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_match,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- sexp_find - Find first subexpression matching pattern
CREATE FUNCTION sexp_find(sexp, sexp)
    RETURNS sexp
    AS 'MODULE_PATHNAME', 'sexp_find_func'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Convenience functions

-- Create nil
CREATE FUNCTION sexp_nil()
    RETURNS sexp
    AS $$ SELECT '()'::sexp $$
    LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- GIN index support
-- These functions enable indexed containment queries using:
--   CREATE INDEX idx ON table USING gin (column sexp_gin_ops);
--   SELECT * FROM table WHERE column @> 'atom';           -- structural
--   SELECT * FROM table WHERE column @>> '(user (name "x"))';  -- key-based

-- Extract keys from stored value (for index building)
CREATE FUNCTION sexp_gin_extract_value(sexp, internal)
    RETURNS internal
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Extract keys from query value (for index searching)
CREATE FUNCTION sexp_gin_extract_query(sexp, internal, int2, internal, internal, internal, internal)
    RETURNS internal
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Check if indexed keys are consistent with query
CREATE FUNCTION sexp_gin_consistent(internal, int2, sexp, int4, internal, internal, internal, internal)
    RETURNS boolean
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Ternary consistent check for GIN (optimization)
CREATE FUNCTION sexp_gin_triconsistent(internal, int2, sexp, int4, internal, internal, internal)
    RETURNS "char"
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- GIN operator class for sexp containment
-- Strategy 7 = @> (structural containment), matching jsonb convention
-- Strategy 9 = @>> (key-based containment), additional operator
CREATE OPERATOR CLASS sexp_gin_ops
    DEFAULT FOR TYPE sexp USING gin AS
    OPERATOR 7 @> (sexp, sexp),
    OPERATOR 9 @>> (sexp, sexp),
    FUNCTION 1 btint4cmp(int4, int4),
    FUNCTION 2 sexp_gin_extract_value(sexp, internal),
    FUNCTION 3 sexp_gin_extract_query(sexp, internal, int2, internal, internal, internal, internal),
    FUNCTION 4 sexp_gin_consistent(internal, int2, sexp, int4, internal, internal, internal, internal),
    FUNCTION 6 sexp_gin_triconsistent(internal, int2, sexp, int4, internal, internal, internal),
    STORAGE int4;

COMMENT ON TYPE sexp IS 'S-expression data type (similar to jsonb but for Lisp-like s-expressions)';
COMMENT ON FUNCTION car(sexp) IS 'Get first element of an s-expression list';
COMMENT ON FUNCTION cdr(sexp) IS 'Get rest of an s-expression list (all but first element)';
COMMENT ON FUNCTION nth(sexp, integer) IS 'Get nth element of an s-expression list (0-indexed)';
COMMENT ON FUNCTION sexp_typeof(sexp) IS 'Get type name of s-expression (nil, symbol, string, integer, float, list)';
COMMENT ON FUNCTION sexp_match(sexp, sexp) IS 'Pattern matching: _ matches any, _* matches rest, ?name captures';
COMMENT ON FUNCTION sexp_find(sexp, sexp) IS 'Find first subexpression matching the pattern';

COMMENT ON OPERATOR CLASS sexp_gin_ops USING gin IS 'GIN index operator class for sexp containment queries';
