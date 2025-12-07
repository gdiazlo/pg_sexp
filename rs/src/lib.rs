//! pg_sexp_rs - S-expression data type for PostgreSQL (Rust implementation)
//!
//! This implementation provides a custom s-expression parser that supports
//! double-quoted strings and stores s-expressions in a compact binary format.

use pgrx::prelude::*;
use pgrx::datum::Internal;
use serde::{Serialize, Deserialize};
use std::fmt;

pgrx::pg_module_magic!();

/// Binary format version for Rust implementation
const FORMAT_VERSION: u8 = 1;

/// Type tags for binary encoding
mod tags {
    pub const NIL: u8 = 0x00;
    pub const INTEGER: u8 = 0x01;
    pub const FLOAT: u8 = 0x02;
    pub const STRING: u8 = 0x03;
    pub const SYMBOL: u8 = 0x04;
    pub const LIST: u8 = 0x05;
    pub const BOOL: u8 = 0x06;
}

// ============================================================================
// Custom S-expression Parser
// ============================================================================

/// Parsed expression (before binary encoding)
#[derive(Debug, Clone, PartialEq)]
enum ParsedExpr {
    Nil,
    Integer(i64),
    Float(f64),
    String(String),
    Symbol(String),
    List(Vec<ParsedExpr>),
}

/// Parse state
struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Parser {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) {
        if self.pos < self.input.len() {
            self.pos += 1;
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.advance();
            } else if c == b';' {
                // Skip line comment
                while let Some(c) = self.peek() {
                    self.advance();
                    if c == b'\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn parse(&mut self) -> Result<ParsedExpr, String> {
        self.skip_whitespace();
        
        match self.peek() {
            None => Ok(ParsedExpr::Nil),
            Some(b'(') => self.parse_list(),
            Some(b'"') => self.parse_string(),
            Some(_) => self.parse_atom(),
        }
    }

    fn parse_list(&mut self) -> Result<ParsedExpr, String> {
        self.advance(); // skip '('
        self.skip_whitespace();
        
        if self.peek() == Some(b')') {
            self.advance();
            return Ok(ParsedExpr::Nil);
        }
        
        let mut items = Vec::new();
        
        while self.peek().is_some() && self.peek() != Some(b')') {
            items.push(self.parse()?);
            self.skip_whitespace();
        }
        
        if self.peek() != Some(b')') {
            return Err("unterminated list".to_string());
        }
        self.advance(); // skip ')'
        
        Ok(ParsedExpr::List(items))
    }

    fn parse_string(&mut self) -> Result<ParsedExpr, String> {
        self.advance(); // skip opening '"'
        let mut s = String::new();
        
        loop {
            match self.peek() {
                None => return Err("unterminated string".to_string()),
                Some(b'"') => {
                    self.advance();
                    break;
                }
                Some(b'\\') => {
                    self.advance();
                    match self.peek() {
                        None => return Err("unterminated string escape".to_string()),
                        Some(b'n') => s.push('\n'),
                        Some(b't') => s.push('\t'),
                        Some(b'r') => s.push('\r'),
                        Some(b'\\') => s.push('\\'),
                        Some(b'"') => s.push('"'),
                        Some(c) => s.push(c as char),
                    }
                    self.advance();
                }
                Some(c) => {
                    s.push(c as char);
                    self.advance();
                }
            }
        }
        
        Ok(ParsedExpr::String(s))
    }

    fn parse_atom(&mut self) -> Result<ParsedExpr, String> {
        let start = self.pos;
        
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() || c == b'(' || c == b')' || c == b'"' || c == b';' {
                break;
            }
            self.advance();
        }
        
        let token = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|_| "invalid UTF-8")?;
        
        if token.is_empty() {
            return Err("empty atom".to_string());
        }
        
        // Check for nil
        if token == "nil" || token == "()" {
            return Ok(ParsedExpr::Nil);
        }
        
        // Try to parse as number
        if let Ok(i) = token.parse::<i64>() {
            return Ok(ParsedExpr::Integer(i));
        }
        
        if let Ok(f) = token.parse::<f64>() {
            return Ok(ParsedExpr::Float(f));
        }
        
        // It's a symbol
        Ok(ParsedExpr::Symbol(token.to_string()))
    }
}

/// PostgreSQL sexp type - stored as varlena binary data
#[derive(PostgresType, Serialize, Deserialize)]
#[inoutfuncs]
pub struct Sexp {
    data: Vec<u8>,
}

impl InOutFuncs for Sexp {
    fn input(input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        let s = input.to_str().expect("invalid UTF-8 in sexp input");
        let s = s.trim();
        
        if s.is_empty() || s == "()" || s == "nil" {
            return Sexp::nil();
        }
        
        let mut parser = Parser::new(s);
        match parser.parse() {
            Ok(parsed) => {
                let mut data = vec![FORMAT_VERSION];
                serialize_parsed(&parsed, &mut data);
                Sexp { data }
            }
            Err(e) => {
                pgrx::error!("invalid s-expression: {}", e);
            }
        }
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        let output = self.to_string_repr();
        buffer.push_str(&output);
    }
}

impl Sexp {
    /// Create a nil (empty list) sexp
    fn nil() -> Self {
        Sexp {
            data: vec![FORMAT_VERSION, tags::NIL],
        }
    }

    /// Convert to string representation
    fn to_string_repr(&self) -> String {
        if self.data.len() < 2 {
            return "()".to_string();
        }
        let mut pos = 1; // skip version
        deserialize_to_string(&self.data, &mut pos)
    }

    /// Get the type of this sexp
    fn get_type(&self) -> SexpType {
        if self.data.len() < 2 {
            return SexpType::Nil;
        }
        match self.data[1] {
            tags::NIL => SexpType::Nil,
            tags::INTEGER => SexpType::Integer,
            tags::FLOAT => SexpType::Float,
            tags::STRING => SexpType::String,
            tags::SYMBOL => SexpType::Symbol,
            tags::LIST => SexpType::List,
            tags::BOOL => SexpType::Bool,
            _ => SexpType::Nil,
        }
    }

    /// Check if this is nil
    fn is_nil(&self) -> bool {
        self.data.len() < 2 || self.data[1] == tags::NIL
    }

    /// Check if this is a list (including nil)
    fn is_list(&self) -> bool {
        self.data.len() < 2 || self.data[1] == tags::NIL || self.data[1] == tags::LIST
    }

    /// Check if this is an atom (not a list)
    fn is_atom(&self) -> bool {
        if self.data.len() < 2 {
            return false;
        }
        matches!(
            self.data[1],
            tags::INTEGER | tags::FLOAT | tags::STRING | tags::SYMBOL | tags::BOOL
        )
    }

    /// Get list length (0 for atoms, 0 for nil)
    fn length(&self) -> i32 {
        if self.data.len() < 2 {
            return 0;
        }
        match self.data[1] {
            tags::NIL => 0,
            tags::LIST => {
                let mut pos = 2;
                read_varint(&self.data, &mut pos) as i32
            }
            _ => 1, // atoms have length 1
        }
    }

    /// Get car (first element) of a list
    fn car(&self) -> Option<Sexp> {
        if !self.is_list() || self.is_nil() {
            return None;
        }
        self.nth(0)
    }

    /// Get cdr (rest) of a list
    fn cdr(&self) -> Option<Sexp> {
        if !self.is_list() || self.is_nil() {
            return None;
        }
        
        let len = self.length();
        if len <= 1 {
            return Some(Sexp::nil());
        }
        
        // Build new list with elements 1..n
        let mut result = vec![FORMAT_VERSION, tags::LIST];
        write_varint(&mut result, (len - 1) as u64);
        
        // Skip to element 1 and copy rest
        let mut pos = 2;
        let _count = read_varint(&self.data, &mut pos);
        skip_element(&self.data, &mut pos); // skip element 0
        
        // Copy remaining elements
        result.extend_from_slice(&self.data[pos..]);
        
        Some(Sexp { data: result })
    }

    /// Get nth element (0-indexed)
    fn nth(&self, n: i32) -> Option<Sexp> {
        if n < 0 {
            return None;
        }
        
        if self.is_atom() {
            return if n == 0 { Some(self.clone()) } else { None };
        }
        
        if self.is_nil() {
            return None;
        }
        
        let mut pos = 2;
        let count = read_varint(&self.data, &mut pos) as i32;
        
        if n >= count {
            return None;
        }
        
        // Skip to nth element
        for _ in 0..n {
            skip_element(&self.data, &mut pos);
        }
        
        // Extract element
        let start = pos;
        skip_element(&self.data, &mut pos);
        let end = pos;
        
        let mut result = vec![FORMAT_VERSION];
        result.extend_from_slice(&self.data[start..end]);
        
        Some(Sexp { data: result })
    }

    /// Check structural containment
    fn contains(&self, needle: &Sexp) -> bool {
        // Check if self equals needle
        if self.equals(needle) {
            return true;
        }
        
        // If self is a list, check children recursively
        if self.data.len() >= 2 && self.data[1] == tags::LIST {
            let mut pos = 2;
            let count = read_varint(&self.data, &mut pos);
            
            for _ in 0..count {
                let start = pos;
                skip_element(&self.data, &mut pos);
                let end = pos;
                
                // Build child sexp and check recursively
                let mut child_data = vec![FORMAT_VERSION];
                child_data.extend_from_slice(&self.data[start..end]);
                let child = Sexp { data: child_data };
                
                if child.contains(needle) {
                    return true;
                }
            }
        }
        
        false
    }

    /// Check equality
    fn equals(&self, other: &Sexp) -> bool {
        // Compare the actual content (skip version byte for comparison)
        if self.data.len() != other.data.len() {
            return false;
        }
        if self.data.len() < 2 {
            return true; // both empty
        }
        self.data[1..] == other.data[1..]
    }

    /// Compute hash for hash indexes
    fn compute_hash(&self) -> i32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        if self.data.len() > 1 {
            self.data[1..].hash(&mut hasher);
        }
        hasher.finish() as i32
    }
}

impl Clone for Sexp {
    fn clone(&self) -> Self {
        Sexp {
            data: self.data.clone(),
        }
    }
}

impl PartialEq for Sexp {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl Eq for Sexp {}

impl std::hash::Hash for Sexp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.data.len() > 1 {
            self.data[1..].hash(state);
        }
    }
}

/// Sexp type enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SexpType {
    Nil,
    Integer,
    Float,
    String,
    Symbol,
    List,
    Bool,
}

impl fmt::Display for SexpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SexpType::Nil => write!(f, "nil"),
            SexpType::Integer => write!(f, "integer"),
            SexpType::Float => write!(f, "float"),
            SexpType::String => write!(f, "string"),
            SexpType::Symbol => write!(f, "symbol"),
            SexpType::List => write!(f, "list"),
            SexpType::Bool => write!(f, "boolean"),
        }
    }
}

// ============================================================================
// Binary Serialization
// ============================================================================

fn serialize_parsed(expr: &ParsedExpr, out: &mut Vec<u8>) {
    match expr {
        ParsedExpr::Nil => {
            out.push(tags::NIL);
        }
        ParsedExpr::Integer(n) => {
            out.push(tags::INTEGER);
            write_signed_varint(out, *n);
        }
        ParsedExpr::Float(f) => {
            out.push(tags::FLOAT);
            out.extend_from_slice(&f.to_le_bytes());
        }
        ParsedExpr::String(s) => {
            out.push(tags::STRING);
            write_string(out, s);
        }
        ParsedExpr::Symbol(s) => {
            out.push(tags::SYMBOL);
            write_string(out, s);
        }
        ParsedExpr::List(items) => {
            if items.is_empty() {
                out.push(tags::NIL);
            } else {
                out.push(tags::LIST);
                write_varint(out, items.len() as u64);
                for item in items {
                    serialize_parsed(item, out);
                }
            }
        }
    }
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn write_signed_varint(out: &mut Vec<u8>, value: i64) {
    // Zigzag encoding
    let encoded = ((value << 1) ^ (value >> 63)) as u64;
    write_varint(out, encoded);
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn read_varint(data: &[u8], pos: &mut usize) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    
    while *pos < data.len() {
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn read_signed_varint(data: &[u8], pos: &mut usize) -> i64 {
    let encoded = read_varint(data, pos);
    // Zigzag decode
    ((encoded >> 1) as i64) ^ (-((encoded & 1) as i64))
}

fn read_string(data: &[u8], pos: &mut usize) -> String {
    let len = read_varint(data, pos) as usize;
    if *pos + len > data.len() {
        return String::new();
    }
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
    *pos += len;
    s
}

fn skip_element(data: &[u8], pos: &mut usize) {
    if *pos >= data.len() {
        return;
    }
    
    let tag = data[*pos];
    *pos += 1;
    
    match tag {
        tags::NIL => {}
        tags::INTEGER => {
            read_varint(data, pos);
        }
        tags::FLOAT => {
            *pos += 8;
        }
        tags::BOOL => {
            *pos += 1;
        }
        tags::STRING | tags::SYMBOL => {
            let len = read_varint(data, pos) as usize;
            *pos += len;
        }
        tags::LIST => {
            let count = read_varint(data, pos);
            for _ in 0..count {
                skip_element(data, pos);
            }
        }
        _ => {}
    }
}

fn deserialize_to_string(data: &[u8], pos: &mut usize) -> String {
    if *pos >= data.len() {
        return "()".to_string();
    }
    
    let tag = data[*pos];
    *pos += 1;
    
    match tag {
        tags::NIL => "()".to_string(),
        tags::INTEGER => {
            let n = read_signed_varint(data, pos);
            n.to_string()
        }
        tags::FLOAT => {
            if *pos + 8 > data.len() {
                return "0.0".to_string();
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
            *pos += 8;
            let f = f64::from_le_bytes(bytes);
            format!("{}", f)
        }
        tags::BOOL => {
            if *pos >= data.len() {
                return "#f".to_string();
            }
            let b = data[*pos] != 0;
            *pos += 1;
            if b { "#t".to_string() } else { "#f".to_string() }
        }
        tags::STRING => {
            let s = read_string(data, pos);
            format!("\"{}\"", escape_string(&s))
        }
        tags::SYMBOL => {
            read_string(data, pos)
        }
        tags::LIST => {
            let count = read_varint(data, pos) as usize;
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                parts.push(deserialize_to_string(data, pos));
            }
            format!("({})", parts.join(" "))
        }
        _ => "()".to_string(),
    }
}

fn escape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\t' => result.push_str("\\t"),
            '\r' => result.push_str("\\r"),
            _ => result.push(c),
        }
    }
    result
}

// ============================================================================
// PostgreSQL Functions
// ============================================================================

/// Get first element of a list
#[pg_extern(name = "car", immutable, parallel_safe)]
fn sexp_car(sexp: Sexp) -> Option<Sexp> {
    sexp.car()
}

/// Get rest of a list (all but first)
#[pg_extern(name = "cdr", immutable, parallel_safe)]
fn sexp_cdr(sexp: Sexp) -> Option<Sexp> {
    sexp.cdr()
}

/// Get nth element (0-indexed)
#[pg_extern(name = "nth", immutable, parallel_safe)]
fn sexp_nth(sexp: Sexp, n: i32) -> Option<Sexp> {
    sexp.nth(n)
}

/// Get length of list
#[pg_extern(name = "sexp_length", immutable, parallel_safe)]
fn sexp_length(sexp: Sexp) -> i32 {
    sexp.length()
}

/// Alias for car
#[pg_extern(name = "head", immutable, parallel_safe)]
fn sexp_head(sexp: Sexp) -> Option<Sexp> {
    sexp.car()
}

/// Get type name
#[pg_extern(name = "sexp_typeof", immutable, parallel_safe)]
fn sexp_typeof(sexp: Sexp) -> String {
    sexp.get_type().to_string()
}

/// Check if nil
#[pg_extern(name = "is_nil", immutable, parallel_safe)]
fn sexp_is_nil(sexp: Sexp) -> bool {
    sexp.is_nil()
}

/// Check if list
#[pg_extern(name = "is_list", immutable, parallel_safe)]
fn sexp_is_list(sexp: Sexp) -> bool {
    sexp.is_list()
}

/// Check if atom
#[pg_extern(name = "is_atom", immutable, parallel_safe)]
fn sexp_is_atom(sexp: Sexp) -> bool {
    sexp.is_atom()
}

/// Check if symbol
#[pg_extern(name = "is_symbol", immutable, parallel_safe)]
fn sexp_is_symbol(sexp: Sexp) -> bool {
    sexp.data.len() >= 2 && sexp.data[1] == tags::SYMBOL
}

/// Check if string
#[pg_extern(name = "is_string", immutable, parallel_safe)]
fn sexp_is_string(sexp: Sexp) -> bool {
    sexp.data.len() >= 2 && sexp.data[1] == tags::STRING
}

/// Check if number
#[pg_extern(name = "is_number", immutable, parallel_safe)]
fn sexp_is_number(sexp: Sexp) -> bool {
    sexp.data.len() >= 2 && matches!(sexp.data[1], tags::INTEGER | tags::FLOAT)
}

/// Equality check
#[pg_extern(name = "sexp_eq", immutable, parallel_safe)]
fn sexp_eq(a: Sexp, b: Sexp) -> bool {
    a.equals(&b)
}

/// Inequality check
#[pg_extern(name = "sexp_ne", immutable, parallel_safe)]
fn sexp_ne(a: Sexp, b: Sexp) -> bool {
    !a.equals(&b)
}

/// Hash function
#[pg_extern(name = "sexp_hash", immutable, parallel_safe)]
fn sexp_hash(sexp: Sexp) -> i32 {
    sexp.compute_hash()
}

/// Extended hash with seed
#[pg_extern(name = "sexp_hash_extended", immutable, parallel_safe)]
fn sexp_hash_extended(sexp: Sexp, seed: i64) -> i64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    if sexp.data.len() > 1 {
        sexp.data[1..].hash(&mut hasher);
    }
    hasher.finish() as i64
}

/// Structural containment (@>)
#[pg_extern(name = "sexp_contains", immutable, parallel_safe)]
fn sexp_contains(container: Sexp, needle: Sexp) -> bool {
    container.contains(&needle)
}

/// Create nil
#[pg_extern(name = "sexp_nil", immutable, parallel_safe)]
fn sexp_nil_func() -> Sexp {
    Sexp::nil()
}

// ============================================================================
// Operators
// ============================================================================

extension_sql!(
    r#"
-- Equality operator
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

-- Inequality operator  
CREATE OPERATOR <> (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_ne,
    COMMUTATOR = <>,
    NEGATOR = =,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);

-- Containment operator (@>)
CREATE OPERATOR @> (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_contains,
    COMMUTATOR = <@,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- Contained by operator (<@)
CREATE FUNCTION sexp_contained(sexp, sexp) RETURNS boolean
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

-- Hash operator class
CREATE OPERATOR CLASS sexp_ops
    DEFAULT FOR TYPE sexp USING hash AS
    OPERATOR 1 = (sexp, sexp),
    FUNCTION 1 sexp_hash(sexp),
    FUNCTION 2 sexp_hash_extended(sexp, bigint);

-- Text casts
CREATE CAST (text AS sexp)
    WITH INOUT
    AS IMPLICIT;

CREATE CAST (sexp AS text)
    WITH INOUT;
"#,
    name = "sexp_operators",
    requires = [sexp_eq, sexp_ne, sexp_contains, sexp_hash, sexp_hash_extended]
);

// ============================================================================
// Key-based Containment (@>> operator)
// ============================================================================

/// Check key-based containment - matches by symbolic keys regardless of structure
/// Container @>> needle means all key-value pairs in needle exist somewhere in container
fn sexp_contains_key_impl(container: &Sexp, needle: &Sexp) -> bool {
    // For atoms, fall back to structural containment
    if needle.is_atom() {
        return container.contains(needle);
    }
    
    // For nil, always matches
    if needle.is_nil() {
        return true;
    }
    
    // For lists, check if it's a key-value pattern
    if needle.data.len() >= 2 && needle.data[1] == tags::LIST {
        let mut pos = 2;
        let count = read_varint(&needle.data, &mut pos) as usize;
        
        if count >= 2 {
            // Check if first element is a symbol (key)
            let first_start = pos;
            if needle.data[first_start] == tags::SYMBOL {
                // This is a (key value ...) pattern
                // Extract the key
                let mut key_pos = first_start + 1;
                let key_len = read_varint(&needle.data, &mut key_pos) as usize;
                let key_bytes = &needle.data[key_pos..key_pos + key_len];
                
                // Skip to second element (value)
                skip_element(&needle.data, &mut pos);
                let value_start = pos;
                let mut value_end = pos;
                skip_element(&needle.data, &mut value_end);
                
                // Build value sexp for comparison
                let mut value_data = vec![FORMAT_VERSION];
                value_data.extend_from_slice(&needle.data[value_start..value_end]);
                let value_sexp = Sexp { data: value_data };
                
                // Search container for matching key-value pair
                if find_key_value_in_container(container, key_bytes, &value_sexp) {
                    // If there are more elements (additional key-value pairs), check them too
                    if count > 2 {
                        // Build a sexp with remaining elements and check recursively
                        for i in 2..count {
                            // Get the i-th element
                            let mut elem_pos = 2;
                            let _ = read_varint(&needle.data, &mut elem_pos); // count
                            for _ in 0..i {
                                skip_element(&needle.data, &mut elem_pos);
                            }
                            let elem_start = elem_pos;
                            skip_element(&needle.data, &mut elem_pos);
                            let elem_end = elem_pos;
                            
                            let mut elem_data = vec![FORMAT_VERSION];
                            elem_data.extend_from_slice(&needle.data[elem_start..elem_end]);
                            let elem_sexp = Sexp { data: elem_data };
                            
                            if !sexp_contains_key_impl(container, &elem_sexp) {
                                return false;
                            }
                        }
                    }
                    return true;
                }
                return false;
            }
        }
        
        // Not a key-value pattern, check all children
        let mut child_pos = pos;
        for _ in 0..count {
            let start = child_pos;
            skip_element(&needle.data, &mut child_pos);
            let end = child_pos;
            
            let mut child_data = vec![FORMAT_VERSION];
            child_data.extend_from_slice(&needle.data[start..end]);
            let child_sexp = Sexp { data: child_data };
            
            if !sexp_contains_key_impl(container, &child_sexp) {
                return false;
            }
        }
        return true;
    }
    
    // Fallback to structural containment
    container.contains(needle)
}

/// Find a key-value pair anywhere in container
fn find_key_value_in_container(container: &Sexp, key_bytes: &[u8], value: &Sexp) -> bool {
    if container.data.len() < 2 {
        return false;
    }
    
    if container.data[1] == tags::LIST {
        let mut pos = 2;
        let count = read_varint(&container.data, &mut pos) as usize;
        
        // Check if this list has the key as first element
        if count >= 2 && container.data[pos] == tags::SYMBOL {
            let mut key_pos = pos + 1;
            let key_len = read_varint(&container.data, &mut key_pos) as usize;
            let container_key = &container.data[key_pos..key_pos + key_len];
            
            if container_key == key_bytes {
                // Key matches, now check if value matches (at any position after the key)
                skip_element(&container.data, &mut pos); // skip key
                
                for _ in 1..count {
                    let val_start = pos;
                    skip_element(&container.data, &mut pos);
                    let val_end = pos;
                    
                    let mut val_data = vec![FORMAT_VERSION];
                    val_data.extend_from_slice(&container.data[val_start..val_end]);
                    let val_sexp = Sexp { data: val_data };
                    
                    // Check structural containment for value
                    if val_sexp.equals(value) || val_sexp.contains(value) {
                        return true;
                    }
                }
            }
        }
        
        // Search recursively in children
        let mut child_pos = 2;
        let _ = read_varint(&container.data, &mut child_pos); // count
        for _ in 0..count {
            let start = child_pos;
            skip_element(&container.data, &mut child_pos);
            let end = child_pos;
            
            let mut child_data = vec![FORMAT_VERSION];
            child_data.extend_from_slice(&container.data[start..end]);
            let child = Sexp { data: child_data };
            
            if find_key_value_in_container(&child, key_bytes, value) {
                return true;
            }
        }
    }
    
    false
}

/// Key-based containment operator (@>>)
#[pg_extern(name = "sexp_contains_key", immutable, parallel_safe)]
fn sexp_contains_key(container: Sexp, needle: Sexp) -> bool {
    sexp_contains_key_impl(&container, &needle)
}

// ============================================================================
// Pattern Matching
// ============================================================================

/// Pattern types for matching
#[derive(Debug, Clone, Copy, PartialEq)]
enum PatternType {
    Literal,
    Wildcard,      // _
    WildcardRest,  // _*
    Capture,       // ?name
    CaptureRest,   // ??name
}

/// Check if a symbol is a pattern symbol
fn get_pattern_type(sym: &str) -> PatternType {
    if sym == "_" {
        PatternType::Wildcard
    } else if sym == "_*" {
        PatternType::WildcardRest
    } else if sym.starts_with("??") {
        PatternType::CaptureRest
    } else if sym.starts_with('?') {
        PatternType::Capture
    } else {
        PatternType::Literal
    }
}

/// Match elements at current positions
fn match_elements(expr_data: &[u8], expr_pos: &mut usize, 
                  pat_data: &[u8], pat_pos: &mut usize) -> bool {
    if *expr_pos >= expr_data.len() || *pat_pos >= pat_data.len() {
        return *expr_pos >= expr_data.len() && *pat_pos >= pat_data.len();
    }
    
    let pat_tag = pat_data[*pat_pos];
    
    // Check if pattern element is a symbol (potential pattern)
    if pat_tag == tags::SYMBOL {
        let saved_pat_pos = *pat_pos;
        *pat_pos += 1;
        let sym_len = read_varint(pat_data, pat_pos) as usize;
        
        if *pat_pos + sym_len <= pat_data.len() {
            let sym = std::str::from_utf8(&pat_data[*pat_pos..*pat_pos + sym_len]).unwrap_or("");
            let ptype = get_pattern_type(sym);
            
            match ptype {
                PatternType::Wildcard | PatternType::Capture => {
                    // Match any single element
                    *pat_pos += sym_len;
                    skip_element(expr_data, expr_pos);
                    return true;
                }
                PatternType::WildcardRest | PatternType::CaptureRest => {
                    // Rest patterns are handled at list level
                    *pat_pos = saved_pat_pos;
                    return false;
                }
                PatternType::Literal => {
                    // Restore and continue with normal matching
                    *pat_pos = saved_pat_pos;
                }
            }
        } else {
            *pat_pos = saved_pat_pos;
        }
    }
    
    // Normal element matching
    let expr_tag = expr_data[*expr_pos];
    let pat_tag = pat_data[*pat_pos];
    
    if expr_tag != pat_tag {
        return false;
    }
    
    match expr_tag {
        tags::NIL => {
            *expr_pos += 1;
            *pat_pos += 1;
            true
        }
        tags::INTEGER => {
            *expr_pos += 1;
            *pat_pos += 1;
            let expr_val = read_signed_varint(expr_data, expr_pos);
            let pat_val = read_signed_varint(pat_data, pat_pos);
            expr_val == pat_val
        }
        tags::FLOAT => {
            if *expr_pos + 9 > expr_data.len() || *pat_pos + 9 > pat_data.len() {
                return false;
            }
            *expr_pos += 1;
            *pat_pos += 1;
            let expr_bytes: [u8; 8] = expr_data[*expr_pos..*expr_pos + 8].try_into().unwrap();
            let pat_bytes: [u8; 8] = pat_data[*pat_pos..*pat_pos + 8].try_into().unwrap();
            *expr_pos += 8;
            *pat_pos += 8;
            expr_bytes == pat_bytes
        }
        tags::STRING => {
            *expr_pos += 1;
            *pat_pos += 1;
            let expr_len = read_varint(expr_data, expr_pos) as usize;
            let pat_len = read_varint(pat_data, pat_pos) as usize;
            
            if expr_len != pat_len {
                *expr_pos += expr_len;
                *pat_pos += pat_len;
                return false;
            }
            
            let result = expr_data[*expr_pos..*expr_pos + expr_len] == 
                         pat_data[*pat_pos..*pat_pos + pat_len];
            *expr_pos += expr_len;
            *pat_pos += pat_len;
            result
        }
        tags::SYMBOL => {
            *expr_pos += 1;
            *pat_pos += 1;
            let expr_len = read_varint(expr_data, expr_pos) as usize;
            let pat_len = read_varint(pat_data, pat_pos) as usize;
            
            if expr_len != pat_len {
                *expr_pos += expr_len;
                *pat_pos += pat_len;
                return false;
            }
            
            let result = expr_data[*expr_pos..*expr_pos + expr_len] == 
                         pat_data[*pat_pos..*pat_pos + pat_len];
            *expr_pos += expr_len;
            *pat_pos += pat_len;
            result
        }
        tags::LIST => {
            *expr_pos += 1;
            *pat_pos += 1;
            match_list_elements(expr_data, expr_pos, pat_data, pat_pos)
        }
        _ => false,
    }
}

/// Match list elements with support for rest patterns
fn match_list_elements(expr_data: &[u8], expr_pos: &mut usize,
                       pat_data: &[u8], pat_pos: &mut usize) -> bool {
    let expr_count = read_varint(expr_data, expr_pos) as usize;
    let pat_count = read_varint(pat_data, pat_pos) as usize;
    
    let mut expr_i = 0;
    let mut pat_i = 0;
    
    while pat_i < pat_count {
        // Check if current pattern element is a rest pattern
        if pat_data[*pat_pos] == tags::SYMBOL {
            let saved_pos = *pat_pos;
            let mut check_pos = *pat_pos + 1;
            let sym_len = read_varint(pat_data, &mut check_pos) as usize;
            
            if check_pos + sym_len <= pat_data.len() {
                let sym = std::str::from_utf8(&pat_data[check_pos..check_pos + sym_len]).unwrap_or("");
                let ptype = get_pattern_type(sym);
                
                if ptype == PatternType::WildcardRest || ptype == PatternType::CaptureRest {
                    // Rest pattern must be last in pattern list
                    if pat_i + 1 != pat_count {
                        return false;
                    }
                    
                    // Consume all remaining expression elements
                    while expr_i < expr_count {
                        skip_element(expr_data, expr_pos);
                        expr_i += 1;
                    }
                    
                    // Skip the rest pattern element
                    *pat_pos = check_pos + sym_len;
                    return true;
                }
            }
            *pat_pos = saved_pos;
        }
        
        // Need exactly one expression element
        if expr_i >= expr_count {
            return false;
        }
        
        // Match this element
        if !match_elements(expr_data, expr_pos, pat_data, pat_pos) {
            return false;
        }
        
        expr_i += 1;
        pat_i += 1;
    }
    
    // All pattern elements matched - check for leftovers
    expr_i == expr_count
}

/// Pattern matching function
#[pg_extern(name = "sexp_match", immutable, parallel_safe)]
fn sexp_match_fn(expr: Sexp, pattern: Sexp) -> bool {
    if expr.data.len() < 2 || pattern.data.len() < 2 {
        return expr.data.len() < 2 && pattern.data.len() < 2;
    }
    
    let mut expr_pos = 1; // skip version
    let mut pat_pos = 1;  // skip version
    
    match_elements(&expr.data, &mut expr_pos, &pattern.data, &mut pat_pos)
}

/// Find first subexpression matching pattern
fn find_pattern_recursive(data: &[u8], pos: &mut usize, pattern: &Sexp) -> Option<Sexp> {
    if *pos >= data.len() {
        return None;
    }
    
    let start = *pos;
    
    // Try matching at current position
    let mut expr_pos = start;
    let mut pat_pos = 1; // skip version in pattern
    
    if match_elements(data, &mut expr_pos, &pattern.data, &mut pat_pos) {
        // Match found! Extract this element
        let mut result_data = vec![FORMAT_VERSION];
        let mut end_pos = start;
        skip_element(data, &mut end_pos);
        result_data.extend_from_slice(&data[start..end_pos]);
        *pos = end_pos;
        return Some(Sexp { data: result_data });
    }
    
    // No match at this position - if it's a list, search children
    let tag = data[*pos];
    *pos += 1;
    
    if tag == tags::LIST {
        let count = read_varint(data, pos) as usize;
        
        for _ in 0..count {
            if let Some(found) = find_pattern_recursive(data, pos, pattern) {
                return Some(found);
            }
        }
    } else {
        // Skip this non-list element
        *pos = start;
        skip_element(data, pos);
    }
    
    None
}

/// Find first subexpression matching pattern
#[pg_extern(name = "sexp_find", immutable, parallel_safe)]
fn sexp_find(expr: Sexp, pattern: Sexp) -> Option<Sexp> {
    if expr.data.len() < 2 {
        return None;
    }
    
    let mut pos = 1; // skip version
    find_pattern_recursive(&expr.data, &mut pos, &pattern)
}

// ============================================================================
// GIN Index Support
// ============================================================================

/// Key type markers for GIN index (must match C implementation for compatibility)
mod gin_keys {
    pub const ATOM: u32 = 0x01000000;
    pub const LIST_HEAD: u32 = 0x02000000;
    pub const SYMBOL: u32 = 0x03000000;
    pub const STRING: u32 = 0x04000000;
    pub const INTEGER: u32 = 0x05000000;
    pub const FLOAT: u32 = 0x06000000;
    pub const PAIR: u32 = 0x07000000;
}

/// Hash combine function (same as C implementation)
fn hash_combine32(seed: u32, hash: u32) -> u32 {
    seed ^ (hash.wrapping_add(0x9e3779b9).wrapping_add(seed << 6).wrapping_add(seed >> 2))
}

/// Compute hash for bytes
fn hash_bytes(data: &[u8]) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish() as u32
}

/// Compute hash for i64
fn hash_i64(val: i64) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish() as u32
}

/// Compute hash for f64
fn hash_f64(val: f64) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    val.to_bits().hash(&mut hasher);
    hasher.finish() as u32
}

/// Make a GIN key with type marker
fn make_gin_key(type_marker: u32, value_hash: u32) -> i32 {
    let combined = type_marker ^ value_hash;
    (combined | 0x80000000) as i32
}

/// Get element hash at position
fn get_element_hash(data: &[u8], pos: &mut usize) -> u32 {
    if *pos >= data.len() {
        return 0;
    }
    
    let tag = data[*pos];
    
    match tag {
        tags::NIL => {
            *pos += 1;
            hash_i64(0)
        }
        tags::INTEGER => {
            *pos += 1;
            let val = read_signed_varint(data, pos);
            hash_i64(val)
        }
        tags::FLOAT => {
            *pos += 1;
            if *pos + 8 > data.len() {
                return 0;
            }
            let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
            *pos += 8;
            let val = f64::from_le_bytes(bytes);
            hash_f64(val)
        }
        tags::STRING => {
            *pos += 1;
            let len = read_varint(data, pos) as usize;
            if *pos + len > data.len() {
                return 0;
            }
            let hash = hash_bytes(&data[*pos..*pos + len]);
            *pos += len;
            hash
        }
        tags::SYMBOL => {
            *pos += 1;
            let len = read_varint(data, pos) as usize;
            if *pos + len > data.len() {
                return 0;
            }
            let hash = hash_bytes(&data[*pos..*pos + len]);
            *pos += len;
            hash
        }
        tags::LIST => {
            *pos += 1;
            let count = read_varint(data, pos);
            if count > 0 {
                // Return hash of first element (head)
                get_element_hash(data, pos)
            } else {
                hash_i64(0)
            }
        }
        _ => 0,
    }
}

/// Extract GIN keys recursively
fn extract_gin_keys(data: &[u8], pos: &mut usize, keys: &mut Vec<i32>, skip_pair_keys: bool) {
    if *pos >= data.len() || keys.len() >= 1024 {
        return;
    }
    
    let tag = data[*pos];
    
    match tag {
        tags::NIL => {
            *pos += 1;
            let hash = hash_i64(0);
            let key = make_gin_key(gin_keys::ATOM, hash);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        tags::INTEGER => {
            let _start = *pos;
            *pos += 1;
            let val = read_signed_varint(data, pos);
            let hash = hash_i64(val);
            let key = make_gin_key(gin_keys::INTEGER, hash);
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        tags::FLOAT => {
            *pos += 1;
            if *pos + 8 <= data.len() {
                let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
                let val = f64::from_le_bytes(bytes);
                let hash = hash_f64(val);
                let key = make_gin_key(gin_keys::FLOAT, hash);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
            *pos += 8;
        }
        tags::STRING => {
            *pos += 1;
            let len = read_varint(data, pos) as usize;
            if *pos + len <= data.len() {
                let hash = hash_bytes(&data[*pos..*pos + len]);
                let key = make_gin_key(gin_keys::STRING, hash);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
            *pos += len;
        }
        tags::SYMBOL => {
            *pos += 1;
            let len = read_varint(data, pos) as usize;
            if *pos + len <= data.len() {
                let hash = hash_bytes(&data[*pos..*pos + len]);
                let key = make_gin_key(gin_keys::SYMBOL, hash);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
            *pos += len;
        }
        tags::LIST => {
            *pos += 1;
            let count = read_varint(data, pos) as usize;
            
            if count == 0 {
                return;
            }
            
            let children_start = *pos;
            
            // Check if this is a 2-element pair with symbol head
            let is_pair = count == 2 && data[*pos] == tags::SYMBOL;
            
            // Get head hash
            let mut head_pos = *pos;
            let head_hash = get_element_hash(data, &mut head_pos);
            
            if is_pair && !skip_pair_keys {
                // Extract pair key: hash(symbol, value)
                let mut second_pos = children_start;
                skip_element(data, &mut second_pos); // skip first element
                let mut second_hash_pos = second_pos;
                let second_hash = get_element_hash(data, &mut second_hash_pos);
                
                let pair_hash = hash_combine32(gin_keys::PAIR, head_hash);
                let pair_hash = hash_combine32(pair_hash, second_hash);
                let key = make_gin_key(gin_keys::PAIR, pair_hash);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            } else if !is_pair {
                // Add list head key for non-pair lists
                let key = make_gin_key(gin_keys::LIST_HEAD, head_hash);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
            
            // Recurse into children
            for _ in 0..count {
                extract_gin_keys(data, pos, keys, skip_pair_keys);
            }
        }
        _ => {
            skip_element(data, pos);
        }
    }
}

/// Extract GIN keys from sexp value (returns array)
#[pg_extern(name = "sexp_extract_keys", immutable, parallel_safe)]
fn sexp_extract_keys(value: Sexp) -> Vec<i32> {
    let mut keys = Vec::new();
    
    if value.data.len() >= 2 {
        let mut pos = 1; // skip version
        extract_gin_keys(&value.data, &mut pos, &mut keys, false);
    }
    
    if keys.is_empty() {
        keys.push(make_gin_key(gin_keys::ATOM, 0));
    }
    
    keys
}

/// Extract GIN keys from query (returns array)
#[pg_extern(name = "sexp_extract_query_keys", immutable, parallel_safe)]
fn sexp_extract_query_keys(query: Sexp, strategy: i32) -> Vec<i32> {
    let mut keys = Vec::new();
    
    // For key-based containment (@>>), skip pair keys
    // Strategy 9 is SEXP_GIN_CONTAINS_KEY_STRATEGY
    let skip_pair_keys = strategy == 9;
    
    if query.data.len() >= 2 {
        let mut pos = 1; // skip version
        extract_gin_keys(&query.data, &mut pos, &mut keys, skip_pair_keys);
    }
    
    if keys.is_empty() {
        keys.push(make_gin_key(gin_keys::ATOM, 0));
    }
    
    keys
}

// ============================================================================
// GIN Index Support (Raw PostgreSQL API)
// ============================================================================

/// GIN strategy numbers (matching C implementation)
const SEXP_GIN_CONTAINS_STRATEGY: i16 = 7;     // @> structural containment
const SEXP_GIN_CONTAINED_STRATEGY: i16 = 8;    // <@ contained by  
const SEXP_GIN_CONTAINS_KEY_STRATEGY: i16 = 9; // @>> key-based containment

/// GIN search modes
const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
const GIN_SEARCH_MODE_ALL: i32 = 2;

/// GIN ternary values
const GIN_FALSE: i8 = 0;
const GIN_TRUE: i8 = 1;
const GIN_MAYBE: i8 = 2;

/// Extract GIN keys from stored value
/// Signature: sexp_gin_extract_value(sexp, internal) -> internal
#[pg_extern(name = "sexp_gin_extract_value", immutable, parallel_safe)]
fn sexp_gin_extract_value_fn(
    value: Sexp,
    nkeys: Internal,
) -> Internal {
    use pgrx::pg_sys;
    
    // Extract keys using our helper function
    let keys = sexp_extract_keys(value);
    let key_count = keys.len();
    
    unsafe {
        // Set nkeys output parameter
        let nkeys_ptr = nkeys.unwrap().unwrap().cast_mut_ptr::<i32>();
        *nkeys_ptr = key_count as i32;
        
        // Allocate Datum array in current memory context
        let datums = pg_sys::palloc(std::mem::size_of::<pg_sys::Datum>() * key_count) 
            as *mut pg_sys::Datum;
        
        // Copy keys as int32 Datums
        for (i, key) in keys.iter().enumerate() {
            *datums.add(i) = pg_sys::Datum::from(*key);
        }
        
        Internal::from(Some(pg_sys::Datum::from(datums)))
    }
}

/// Extract GIN keys from query value
/// Signature: sexp_gin_extract_query(sexp, internal, int2, internal, internal, internal, internal) -> internal
#[pg_extern(name = "sexp_gin_extract_query", immutable, parallel_safe)]
fn sexp_gin_extract_query_fn(
    query: Sexp,
    nkeys: Internal,
    strategy: i16,
    _pmatch: Internal,
    _extra_data: Internal,
    _null_flags: Internal,
    search_mode: Internal,
) -> Internal {
    use pgrx::pg_sys;
    
    // Handle contained-by strategy specially
    if strategy == SEXP_GIN_CONTAINED_STRATEGY {
        unsafe {
            let nkeys_ptr = nkeys.unwrap().unwrap().cast_mut_ptr::<i32>();
            *nkeys_ptr = 0;
            let search_mode_ptr = search_mode.unwrap().unwrap().cast_mut_ptr::<i32>();
            *search_mode_ptr = GIN_SEARCH_MODE_ALL;
        }
        return Internal::default();
    }
    
    // Extract keys using our helper function with appropriate strategy
    let keys = sexp_extract_query_keys(query, strategy as i32);
    let key_count = keys.len();
    
    unsafe {
        // Set nkeys output parameter
        let nkeys_ptr = nkeys.unwrap().unwrap().cast_mut_ptr::<i32>();
        *nkeys_ptr = key_count as i32;
        
        // Set search mode
        let search_mode_ptr = search_mode.unwrap().unwrap().cast_mut_ptr::<i32>();
        *search_mode_ptr = GIN_SEARCH_MODE_DEFAULT;
        
        // Allocate Datum array
        let datums = pg_sys::palloc(std::mem::size_of::<pg_sys::Datum>() * key_count)
            as *mut pg_sys::Datum;
        
        // Copy keys as int32 Datums
        for (i, key) in keys.iter().enumerate() {
            *datums.add(i) = pg_sys::Datum::from(*key);
        }
        
        Internal::from(Some(pg_sys::Datum::from(datums)))
    }
}

/// GIN consistent check
/// Signature: sexp_gin_consistent(internal, int2, sexp, int4, internal, internal, internal, internal) -> bool
#[pg_extern(name = "sexp_gin_consistent", immutable, parallel_safe)]
fn sexp_gin_consistent_fn(
    check: Internal,
    strategy: i16,
    _query: Sexp,
    nkeys: i32,
    _extra_data: Internal,
    recheck: Internal,
    _query_keys: Internal,
    _null_flags: Internal,
) -> bool {
    use pgrx::pg_sys;
    
    unsafe {
        // Always require recheck (keys are hashes, collisions possible)
        let recheck_ptr = recheck.unwrap().unwrap().cast_mut_ptr::<bool>();
        *recheck_ptr = true;
        
        let check_ptr = check.unwrap().unwrap().cast_mut_ptr::<bool>();
        
        match strategy {
            SEXP_GIN_CONTAINS_STRATEGY | SEXP_GIN_CONTAINS_KEY_STRATEGY => {
                // All query keys must be present
                for i in 0..nkeys {
                    if !*check_ptr.add(i as usize) {
                        return false;
                    }
                }
                true
            }
            SEXP_GIN_CONTAINED_STRATEGY => {
                // For contained-by, we can't efficiently pre-filter
                true
            }
            _ => {
                pgrx::error!("sexp_gin_consistent: unknown strategy {}", strategy);
            }
        }
    }
}

/// GIN triconsistent check (ternary logic)
/// Signature: sexp_gin_triconsistent(internal, int2, sexp, int4, internal, internal, internal) -> char
#[pg_extern(name = "sexp_gin_triconsistent", immutable, parallel_safe)]
fn sexp_gin_triconsistent_fn(
    check: Internal,
    strategy: i16,
    _query: Sexp,
    nkeys: i32,
    _extra_data: Internal,
    _query_keys: Internal,
    _null_flags: Internal,
) -> i8 {
    use pgrx::pg_sys;
    
    unsafe {
        let check_ptr = check.unwrap().unwrap().cast_mut_ptr::<i8>();
        
        let mut all_true = true;
        let mut any_false = false;
        
        for i in 0..nkeys {
            let val = *check_ptr.add(i as usize);
            if val == GIN_FALSE {
                any_false = true;
                all_true = false;
                break;
            } else if val == GIN_MAYBE {
                all_true = false;
            }
        }
        
        match strategy {
            SEXP_GIN_CONTAINS_STRATEGY | SEXP_GIN_CONTAINS_KEY_STRATEGY => {
                if any_false {
                    GIN_FALSE
                } else if all_true && nkeys == 1 {
                    // Single-key optimization: skip recheck for single atom queries
                    GIN_TRUE
                } else {
                    GIN_MAYBE
                }
            }
            SEXP_GIN_CONTAINED_STRATEGY => {
                GIN_MAYBE
            }
            _ => {
                pgrx::error!("sexp_gin_triconsistent: unknown strategy {}", strategy);
            }
        }
    }
}

// ============================================================================
// Additional Operators
// ============================================================================

extension_sql!(
    r#"
-- Key-based containment operator (@>>)
CREATE OPERATOR @>> (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_contains_key,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- Pattern match operator (~)
CREATE OPERATOR ~ (
    LEFTARG = sexp,
    RIGHTARG = sexp,
    FUNCTION = sexp_match,
    RESTRICT = contsel,
    JOIN = contjoinsel
);

-- GIN operator class for sexp containment
-- Strategy 7 = @> (structural containment), matching jsonb convention
-- Strategy 9 = @>> (key-based containment)
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

COMMENT ON OPERATOR CLASS sexp_gin_ops USING gin IS 'GIN index operator class for sexp containment queries';
"#,
    name = "sexp_additional_operators",
    requires = [
        "sexp_operators",
        sexp_contains_key, 
        sexp_match_fn, 
        sexp_extract_keys, 
        sexp_extract_query_keys,
        sexp_gin_extract_value_fn,
        sexp_gin_extract_query_fn,
        sexp_gin_consistent_fn,
        sexp_gin_triconsistent_fn
    ]
);

// ============================================================================
// Tests
// ============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_parse_symbol() {
        let s = Sexp::input(c"foo");
        assert_eq!(s.get_type(), SexpType::Symbol);
        assert_eq!(s.to_string_repr(), "foo");
    }

    #[pg_test]
    fn test_parse_integer() {
        let s = Sexp::input(c"42");
        assert_eq!(s.get_type(), SexpType::Integer);
        assert_eq!(s.to_string_repr(), "42");
    }

    #[pg_test]
    fn test_parse_negative() {
        let s = Sexp::input(c"-123");
        assert_eq!(s.get_type(), SexpType::Integer);
        assert_eq!(s.to_string_repr(), "-123");
    }

    #[pg_test]
    fn test_parse_float() {
        let s = Sexp::input(c"3.14");
        assert_eq!(s.get_type(), SexpType::Float);
    }

    #[pg_test]
    fn test_parse_nil() {
        let s = Sexp::input(c"()");
        assert!(s.is_nil());
        assert_eq!(s.to_string_repr(), "()");
    }

    #[pg_test]
    fn test_parse_list() {
        let s = Sexp::input(c"(foo bar baz)");
        assert_eq!(s.get_type(), SexpType::List);
        assert_eq!(s.length(), 3);
    }

    #[pg_test]
    fn test_car_cdr() {
        let s = Sexp::input(c"(a b c)");
        
        let car = s.car().unwrap();
        assert_eq!(car.to_string_repr(), "a");
        
        let cdr = s.cdr().unwrap();
        assert_eq!(cdr.length(), 2);
    }

    #[pg_test]
    fn test_nth() {
        let s = Sexp::input(c"(a b c d)");
        
        assert_eq!(s.nth(0).unwrap().to_string_repr(), "a");
        assert_eq!(s.nth(1).unwrap().to_string_repr(), "b");
        assert_eq!(s.nth(2).unwrap().to_string_repr(), "c");
        assert_eq!(s.nth(3).unwrap().to_string_repr(), "d");
        assert!(s.nth(4).is_none());
    }

    #[pg_test]
    fn test_contains() {
        let container = Sexp::input(c"(a (b c) d)");
        let needle = Sexp::input(c"(b c)");
        let not_there = Sexp::input(c"(x y)");
        
        assert!(container.contains(&needle));
        assert!(!container.contains(&not_there));
    }

    #[pg_test]
    fn test_equality() {
        let a = Sexp::input(c"(foo bar)");
        let b = Sexp::input(c"(foo bar)");
        let c = Sexp::input(c"(foo baz)");
        
        assert!(a.equals(&b));
        assert!(!a.equals(&c));
    }

    #[pg_test]
    fn test_pattern_match_wildcard() {
        let expr = Sexp::input(c"(foo bar baz)");
        let pattern = Sexp::input(c"(foo _ baz)");
        
        assert!(sexp_match_fn(expr, pattern));
    }

    #[pg_test]
    fn test_pattern_match_rest() {
        let expr = Sexp::input(c"(foo a b c d)");
        let pattern = Sexp::input(c"(foo _*)");
        
        assert!(sexp_match_fn(expr, pattern));
    }

    #[pg_test]
    fn test_pattern_no_match() {
        let expr = Sexp::input(c"(foo bar)");
        let pattern = Sexp::input(c"(foo baz)");
        
        assert!(!sexp_match_fn(expr, pattern));
    }

    #[pg_test]
    fn test_key_containment() {
        let container = Sexp::input(c"(user (id 100) (name \"John\") (age 30))");
        let needle = Sexp::input(c"(name \"John\")");
        
        assert!(sexp_contains_key_impl(&container, &needle));
    }

    #[pg_test]
    fn test_key_containment_nested() {
        let container = Sexp::input(c"(data (user (id 100)))");
        let needle = Sexp::input(c"(id 100)");
        
        assert!(sexp_contains_key_impl(&container, &needle));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
