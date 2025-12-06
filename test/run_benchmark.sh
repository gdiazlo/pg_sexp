#!/bin/bash
#
# pg_sexp Benchmark Runner
# Starts container, runs benchmarks, and generates reports
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONTAINER_NAME="pg_sexp-bench-$$"
IMAGE_NAME="${IMAGE_NAME:-localhost/pg_sexp-production}"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="$RESULTS_DIR/benchmark_${TIMESTAMP}.txt"
SUMMARY_FILE="$RESULTS_DIR/summary_${TIMESTAMP}.txt"
KEEP_CONTAINER=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Run pg_sexp benchmarks and generate reports.

This script will:
  1. Start a fresh PostgreSQL container with pg_sexp installed
  2. Run the benchmark suite
  3. Generate a summary report
  4. Clean up the container (unless --keep is specified)

OPTIONS:
    -i, --image IMAGE       Container image (default: localhost/pg_sexp-production)
    -q, --quick             Run quick benchmark (reduced row counts)
    -o, --output FILE       Output file for raw results
    -k, --keep              Keep container running after benchmark
    -h, --help              Show this help message
    --compare FILE          Compare with previous benchmark results
    --list                  List available benchmark results

EXAMPLES:
    $(basename "$0")                    # Run full benchmark
    $(basename "$0") --quick            # Run quick benchmark  
    $(basename "$0") --keep             # Keep container for inspection
    $(basename "$0") --compare results/benchmark_20241201_120000.txt
    $(basename "$0") --list             # List previous results

EOF
    exit 0
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Cleanup function
cleanup() {
    if [[ "$KEEP_CONTAINER" == "false" ]] && podman ps -a --format "{{.Names}}" 2>/dev/null | grep -q "^${CONTAINER_NAME}$"; then
        log_info "Cleaning up container '$CONTAINER_NAME'..."
        podman rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    fi
}

# Set trap for cleanup
trap cleanup EXIT

# Check if image exists
check_image() {
    if ! podman image exists "$IMAGE_NAME" 2>/dev/null; then
        log_error "Image '$IMAGE_NAME' not found"
        log_info "Build it with: podman build -f Containerfile.production -t pg_sexp-production ."
        exit 1
    fi
    log_success "Image '$IMAGE_NAME' found"
}

# Start container
start_container() {
    log_info "Starting container '$CONTAINER_NAME'..."
    
    # Remove any existing container with same name
    podman rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    
    # Start container (no port exposure as per user constraint)
    if ! podman run -d --name "$CONTAINER_NAME" "$IMAGE_NAME" >/dev/null 2>&1; then
        log_error "Failed to start container"
        exit 1
    fi
    
    log_success "Container started"
    
    # Wait for PostgreSQL to be ready
    log_info "Waiting for PostgreSQL to be ready..."
    local max_attempts=30
    local attempt=0
    
    while [[ $attempt -lt $max_attempts ]]; do
        if podman exec "$CONTAINER_NAME" pg_isready -U postgres >/dev/null 2>&1; then
            log_success "PostgreSQL is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    
    log_error "PostgreSQL failed to start within ${max_attempts} seconds"
    exit 1
}

# Ensure results directory exists
setup_results_dir() {
    mkdir -p "$RESULTS_DIR"
    log_success "Results directory: $RESULTS_DIR"
}

# Run the benchmark
run_benchmark() {
    local sql_file="$1"
    local output="$2"
    
    log_info "Running benchmark (this may take several minutes)..."
    log_info "Output: $output"
    echo ""
    
    # Run benchmark and capture output
    if ! podman exec -i "$CONTAINER_NAME" psql -U postgres -d postgres < "$sql_file" > "$output" 2>&1; then
        log_error "Benchmark failed!"
        cat "$output"
        exit 1
    fi
    
    log_success "Benchmark completed"
}

# Extract timing results and generate summary
generate_summary() {
    local input="$1"
    local summary="$2"
    
    log_info "Generating summary: $summary"
    
    cat > "$summary" <<EOF
================================================================================
pg_sexp Benchmark Summary
Generated: $(date)
Image: $IMAGE_NAME
================================================================================

EOF

    # Extract PostgreSQL version
    echo "## POSTGRESQL VERSION" >> "$summary"
    grep -E "PostgreSQL [0-9]+" "$input" | head -1 | sed 's/^[ ]*//' >> "$summary" 2>/dev/null || true
    echo "" >> "$summary"

    # Extract storage comparison
    echo "## STORAGE COMPARISON" >> "$summary"
    echo "" >> "$summary"
    # Get table sizes section
    awk '/--- Table and index sizes ---/,/^$/' "$input" | grep -E "^\s*(sexp|jsonb)" >> "$summary" 2>/dev/null || true
    echo "" >> "$summary"
    # Get average row sizes
    awk '/--- Average row size/,/^$/' "$input" | grep -E "^\s*(sexp|jsonb)" >> "$summary" 2>/dev/null || true
    echo "" >> "$summary"

    # Extract timing results - only actual test timings, skip setup
    echo "## TIMING RESULTS" >> "$summary"
    echo "" >> "$summary"
    printf "%-65s %12s\n" "Test" "Time (ms)" >> "$summary"
    printf "%-65s %12s\n" "$(printf '%.0s-' {1..65})" "$(printf '%.0s-' {1..12})" >> "$summary"
    
    # Parse timing lines - look for test markers followed by Time:
    awk '
    /^--- [^-]/ { 
        gsub(/^--- /, "", $0)
        gsub(/ ---$/, "", $0)
        current_test = $0
    }
    /^Time:/ && current_test != "" {
        time = $0
        gsub(/Time: /, "", time)
        gsub(/ ms.*/, "", time)
        gsub(/^[ \t]+|[ \t]+$/, "", time)
        if (time + 0 > 0) {
            printf "%-65s %12s\n", substr(current_test, 1, 65), time
        }
        current_test = ""
    }
    ' "$input" >> "$summary"
    
    echo "" >> "$summary"
    
    # Extract GIN index sizes
    echo "## GIN INDEX SIZES" >> "$summary"
    echo "" >> "$summary"
    awk '/--- GIN Index sizes ---/,/^$/' "$input" | grep -E "^\s*(sexp|jsonb)" >> "$summary" 2>/dev/null || true
    echo "" >> "$summary"

    # Extract AST storage sizes
    echo "## AST STORAGE SIZES" >> "$summary"
    echo "" >> "$summary"
    awk '/--- AST storage sizes ---/,/^$/' "$input" | grep -E "^\s*(sexp|jsonb)" >> "$summary" 2>/dev/null || true
    echo "" >> "$summary"

    # Extract deep nesting storage sizes
    echo "## DEEP NESTING STORAGE SIZES" >> "$summary"
    echo "" >> "$summary"
    awk '/--- Deep nesting storage sizes ---/,/^$/' "$input" | grep -E "^\s*(sexp|jsonb)" >> "$summary" 2>/dev/null || true
    echo "" >> "$summary"

    # Performance summary comparison - pair up sexp/jsonb tests
    echo "## PERFORMANCE COMPARISON (sexp vs jsonb)" >> "$summary"
    echo "" >> "$summary"
    printf "%-50s %12s %12s %10s\n" "Operation" "sexp (ms)" "jsonb (ms)" "Ratio" >> "$summary"
    printf "%-50s %12s %12s %10s\n" "$(printf '%.0s-' {1..50})" "$(printf '%.0s-' {1..12})" "$(printf '%.0s-' {1..12})" "$(printf '%.0s-' {1..10})" >> "$summary"
    
    # Extract paired comparisons
    awk '
    /^--- sexp[^@]*:/ { 
        gsub(/^--- /, "", $0)
        gsub(/ ---$/, "", $0)
        sexp_test = $0
        sexp_time = ""
    }
    /^Time:/ && sexp_test != "" && sexp_time == "" {
        sexp_time = $0
        gsub(/Time: /, "", sexp_time)
        gsub(/ ms.*/, "", sexp_time)
    }
    /^--- jsonb[^@]*:/ { 
        gsub(/^--- /, "", $0)
        gsub(/ ---$/, "", $0)
        jsonb_test = $0
        jsonb_time = ""
    }
    /^Time:/ && jsonb_test != "" && jsonb_time == "" {
        jsonb_time = $0
        gsub(/Time: /, "", jsonb_time)
        gsub(/ ms.*/, "", jsonb_time)
        
        # Print comparison if we have both times
        if (sexp_time != "" && jsonb_time != "" && sexp_time + 0 > 0) {
            ratio = (jsonb_time + 0) / (sexp_time + 0)
            # Extract operation description
            desc = sexp_test
            gsub(/^sexp[^:]*: /, "", desc)
            printf "%-50s %12s %12s %9.2fx\n", substr(desc, 1, 50), sexp_time, jsonb_time, ratio
        }
        sexp_test = ""
        sexp_time = ""
        jsonb_test = ""
        jsonb_time = ""
    }
    ' "$input" >> "$summary" 2>/dev/null || true
    
    cat >> "$summary" <<EOF

================================================================================
Full results: $input
================================================================================
EOF

    log_success "Summary generated"
}

# Compare two benchmark results
compare_results() {
    local old="$1"
    local new="$2"
    
    if [[ ! -f "$old" ]]; then
        log_error "Comparison file not found: $old"
        exit 1
    fi
    
    echo ""
    echo "================================================================================"
    echo "BENCHMARK COMPARISON"
    echo "================================================================================"
    echo "Old: $old"
    echo "New: $new"
    echo ""
    
    # Extract timings from both files
    echo "## TIMING COMPARISON (ms)"
    echo ""
    printf "%-50s %12s %12s %10s\n" "Test" "Old" "New" "Change"
    printf "%-50s %12s %12s %10s\n" "----" "---" "---" "------"
    
    # Create temp files with test->timing mappings
    local old_times=$(mktemp)
    local new_times=$(mktemp)
    
    awk '
    /^---.*---$/ { 
        gsub(/^--- /, "", $0)
        gsub(/ ---$/, "", $0)
        current_test = $0
    }
    /^Time:/ {
        gsub(/Time: /, "", $0)
        gsub(/ ms.*/, "", $0)
        print current_test "\t" $0
    }
    ' "$old" > "$old_times"
    
    awk '
    /^---.*---$/ { 
        gsub(/^--- /, "", $0)
        gsub(/ ---$/, "", $0)
        current_test = $0
    }
    /^Time:/ {
        gsub(/Time: /, "", $0)
        gsub(/ ms.*/, "", $0)
        print current_test "\t" $0
    }
    ' "$new" > "$new_times"
    
    # Compare
    while IFS=$'\t' read -r test new_time; do
        old_time=$(grep -F "$test" "$old_times" 2>/dev/null | cut -f2 | head -1)
        if [[ -n "$old_time" && -n "$new_time" ]]; then
            # Calculate percentage change
            change=$(awk -v old="$old_time" -v new="$new_time" 'BEGIN {
                if (old > 0) {
                    pct = ((new - old) / old) * 100
                    if (pct > 0) printf "+%.1f%%", pct
                    else printf "%.1f%%", pct
                } else {
                    print "N/A"
                }
            }')
            printf "%-50s %12s %12s %10s\n" "${test:0:50}" "$old_time" "$new_time" "$change"
        fi
    done < "$new_times"
    
    rm -f "$old_times" "$new_times"
    echo ""
}

# List available results
list_results() {
    if [[ ! -d "$RESULTS_DIR" ]] || [[ -z "$(ls -A "$RESULTS_DIR" 2>/dev/null)" ]]; then
        log_info "No benchmark results found in $RESULTS_DIR"
        exit 0
    fi
    
    echo "Available benchmark results:"
    echo ""
    for f in $(ls -t "$RESULTS_DIR"/benchmark_*.txt 2>/dev/null | head -20); do
        local size=$(du -h "$f" | cut -f1)
        local date=$(basename "$f" | sed 's/benchmark_\(quick_\)\?\([0-9]*\)_\([0-9]*\).txt/\2 \3/' | sed 's/\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\) \([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/\1-\2-\3 \4:\5:\6/')
        echo "  $f ($size, $date)"
    done
    echo ""
}

# Create quick benchmark variant
create_quick_benchmark() {
    local output="$1"
    
    # Create a modified benchmark with smaller row counts
    sed -e 's/500000/50000/g' \
        -e 's/500,000/50,000/g' \
        -e 's/200000/20000/g' \
        -e 's/200,000/20,000/g' \
        -e 's/100000/10000/g' \
        -e 's/100,000/10,000/g' \
        "$SCRIPT_DIR/benchmark.sql" > "$output"
    
    log_info "Created quick benchmark with reduced row counts (10%)"
}

# Main
main() {
    local quick_mode=false
    local compare_file=""
    local list_mode=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -i|--image)
                IMAGE_NAME="$2"
                shift 2
                ;;
            -q|--quick)
                quick_mode=true
                shift
                ;;
            -o|--output)
                OUTPUT_FILE="$2"
                shift 2
                ;;
            -k|--keep)
                KEEP_CONTAINER=true
                shift
                ;;
            --compare)
                compare_file="$2"
                shift 2
                ;;
            --list)
                list_mode=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done
    
    if $list_mode; then
        list_results
        exit 0
    fi
    
    echo ""
    echo "================================================================================"
    echo "pg_sexp Benchmark Runner"
    echo "================================================================================"
    echo ""
    
    check_image
    setup_results_dir
    start_container
    
    local sql_file="$SCRIPT_DIR/benchmark.sql"
    
    if $quick_mode; then
        sql_file=$(mktemp --suffix=.sql)
        create_quick_benchmark "$sql_file"
        OUTPUT_FILE="$RESULTS_DIR/benchmark_quick_${TIMESTAMP}.txt"
        SUMMARY_FILE="$RESULTS_DIR/summary_quick_${TIMESTAMP}.txt"
    fi
    
    run_benchmark "$sql_file" "$OUTPUT_FILE"
    generate_summary "$OUTPUT_FILE" "$SUMMARY_FILE"
    
    if $quick_mode; then
        rm -f "$sql_file"
    fi
    
    echo ""
    log_success "Benchmark complete!"
    echo ""
    echo "Results:"
    echo "  Full output: $OUTPUT_FILE"
    echo "  Summary:     $SUMMARY_FILE"
    
    if [[ "$KEEP_CONTAINER" == "true" ]]; then
        echo "  Container:   $CONTAINER_NAME (kept running)"
    fi
    echo ""
    
    # Show summary
    echo "================================================================================"
    echo "SUMMARY"
    echo "================================================================================"
    cat "$SUMMARY_FILE"
    
    # Compare if requested
    if [[ -n "$compare_file" ]]; then
        compare_results "$compare_file" "$OUTPUT_FILE"
    fi
}

main "$@"
