#!/bin/bash
# =============================================================================
# LDBC SNB Correctness Checker (per-param mode)
# =============================================================================
# Reads config lines of the form "query=dataset", downloads the dataset from
# HuggingFace if not cached, then runs each query with DuckDB and Flowlog
# one param row at a time, verifying all results match.
#
# Usage:
#   bash tools/check/compare_ldbc.sh [--config <file>] [--param_num <n>] [--timeout <s>]
#   --config     config file (default: tools/config/config_ldbc.txt)
#   --param_num  max param rows per query, 0 = all (default: 0)
#   --timeout    per-param timeout in seconds (default: 300)
#
# Environment variables:
#   DUCKDB_BIN  - path to duckdb binary (default: duckdb on PATH, then /tmp/duckdb)
#   WORKERS     - parallelism for both engines (default: 64)
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
log()  { echo -e "${CYAN}[CHECK]${NC} $*"; }
pass() { echo -e "${GREEN}[PASS]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

CONFIG="${ROOT_DIR}/tools/config/config_ldbc.txt"
MAX_PARAMS=0
TIMEOUT_SECS=300

while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)              CONFIG="$2";       shift 2 ;;
        --param_num)           MAX_PARAMS="$2";   shift 2 ;;
        --timeout|--time_out)  TIMEOUT_SECS="$2"; shift 2 ;;
        *) die "Unknown argument: $1" ;;
    esac
done

HF_BASE="https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main"
FACT_DIR="${ROOT_DIR}/facts/ldbc"

FLOWLOG_BIN="${ROOT_DIR}/target/release/flowlog"
DUCKDB_BIN="${DUCKDB_BIN:-$(command -v duckdb 2>/dev/null || echo /tmp/duckdb)}"

DL_DIR="${ROOT_DIR}/example/ldbc_snb/flowlog"
SQL_DIR="${ROOT_DIR}/example/ldbc_snb/duckdb"

WORK_DIR="/tmp/ldbc_compare"
mkdir -p "$WORK_DIR" "$FACT_DIR"

fmt_ms() {
    local ms=${1:-0}
    if [[ $ms -ge 1000 ]]; then
        printf "%.2fs" "$(echo "scale=2; $ms/1000" | bc)"
    else
        printf "%dms" "$ms"
    fi
}

trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

[[ -f "$CONFIG" ]]     || die "Config not found: $CONFIG"
[[ -x "$DUCKDB_BIN" ]] || die "duckdb not found: $DUCKDB_BIN"
[[ -x "$FLOWLOG_BIN" ]] || die "flowlog not built: $FLOWLOG_BIN"

WORKERS="${WORKERS:-64}"

# ── Dataset management ────────────────────────────────────────────────────────
setup_dataset() {
    local dataset_name="$1"
    local extract_path="${FACT_DIR}/${dataset_name}"

    if [[ -d "$extract_path" ]]; then
        log "Dataset $dataset_name already cached"
        return
    fi

    # If a pre-built directory exists in /dev/shm, symlink it instead of downloading
    if [[ -d "/dev/shm/${dataset_name}" ]]; then
        log "Dataset $dataset_name found in /dev/shm, symlinking"
        ln -s "/dev/shm/${dataset_name}" "$extract_path"
        return
    fi

    command -v wget >/dev/null 2>&1 || die "wget not found; cannot download datasets"

    # Try .tar.zst first, fall back to .tar.gz
    local dataset_tar_zst="/dev/shm/${dataset_name}.tar.zst"
    local dataset_tar_gz="/dev/shm/${dataset_name}.tar.gz"

    if wget -q --spider "${HF_BASE}/dataset/ldbc/${dataset_name}.tar.zst" 2>/dev/null; then
        log "Downloading $dataset_name (.tar.zst) ..."
        wget -q -O "$dataset_tar_zst" "${HF_BASE}/dataset/ldbc/${dataset_name}.tar.zst" \
            || die "Download failed"
        log "Extracting $dataset_name ..."
        tar --use-compress-program=zstd -xf "$dataset_tar_zst" -C "$FACT_DIR"
        rm -f "$dataset_tar_zst"
    else
        log "Downloading $dataset_name (.tar.gz) ..."
        wget -q -O "$dataset_tar_gz" "${HF_BASE}/dataset/ldbc/${dataset_name}.tar.gz" \
            || die "Download failed: ${HF_BASE}/dataset/ldbc/${dataset_name}.tar.gz"
        log "Extracting $dataset_name ..."
        tar -xzf "$dataset_tar_gz" -C "$FACT_DIR"
        rm -f "$dataset_tar_gz"
    fi

    log "Dataset $dataset_name ready at $extract_path"
}

cleanup_dataset() {
    local dataset_name="$1"
    local extract_path="${FACT_DIR}/${dataset_name}"
    log "Cleaning up dataset $dataset_name"
    if [[ -L "$extract_path" ]]; then
        rm -f "$extract_path"
        rm -rf "/dev/shm/${dataset_name}"
    else
        rm -rf "$extract_path"
    fi
    rm -f "/dev/shm/${dataset_name}.tar.zst" "/dev/shm/${dataset_name}.tar.gz"
}

# ── Per-param runner ──────────────────────────────────────────────────────────
run_per_param() {
    local query="$1" data_dir="$2"
    local dl_file="${DL_DIR}/${query}.dl"
    local sql_file="${SQL_DIR}/${query}.sql"
    local qwork="${WORK_DIR}/${query}"
    local fl_proj="${qwork}/fl_proj"
    local fl_out_dir="${qwork}/fl_out"
    mkdir -p "$qwork"

    # Detect param filename from DL
    local param_fname
    param_fname=$(grep 'filename=' "$dl_file" | grep -i param | head -1 \
                  | sed 's/.*filename="\([^"]*\)".*/\1/')
    local param_file="${data_dir}/${param_fname}"
    [[ -f "$param_file" ]] || { fail "$query: param file not found: $param_file"; return 1; }

    # ── Compile Flowlog once ──
    log "$query: compiling Flowlog..."
    rm -rf "$fl_proj" "$fl_out_dir"
    mkdir -p "$fl_out_dir"
    "$FLOWLOG_BIN" "$dl_file" -F "$data_dir" -D "$fl_out_dir" -o "$fl_proj" --str-intern >/dev/null 2>&1
    (cd "$fl_proj" && cargo build --release 2>/dev/null)
    local fl_bin
    fl_bin=$(find "$fl_proj/target/release" -maxdepth 1 -type f -executable | grep -v '\.d$' | head -1)
    local fl_out_hardcoded
    fl_out_hardcoded=$(grep 'base_dir = ' "$fl_proj/src/main.rs" | grep -v '//' | head -1 \
                       | sed 's/.*"\(.*\)".*/\1/')
    mkdir -p "$fl_out_hardcoded"

    # ── Load params ──
    local header
    header=$(head -1 "$param_file")
    mapfile -t param_rows < <(tail -n +2 "$param_file" | grep -v '^$')
    local total=${#param_rows[@]}
    if [[ "$MAX_PARAMS" -gt 0 && "$MAX_PARAMS" -lt "$total" ]]; then
        param_rows=("${param_rows[@]:0:$MAX_PARAMS}")
        total=$MAX_PARAMS
    fi
    log "$query: running $total params (per-param)..."

    local orig_backup="${qwork}/param_backup.txt"
    cp "$param_file" "$orig_backup"
    trap "cp '$orig_backup' '$param_file' 2>/dev/null; trap - EXIT INT TERM" EXIT INT TERM

    local fl_times=() db_times=()
    local timeout_rows=() mismatch_rows=()
    local total_rows=0
    local sql_subst idx=0
    sql_subst=$(sed "s|:dataDir|'${data_dir}'|g" "$sql_file")

    for row in "${param_rows[@]}"; do
        idx=$(( idx + 1 ))
        printf '%s\n%s\n' "$header" "$row" > "$param_file"

        local fl_param_out="${qwork}/fl_${idx}.txt"
        local db_param_out="${qwork}/db_${idx}.csv"
        local fl_ok=true db_ok=true
        local _t0 _t1

        # Flowlog
        find "$fl_out_hardcoded" -maxdepth 1 -type f -delete 2>/dev/null || true
        printf "\r${CYAN}[CHECK]${NC} Flowlog  [%d/%d]  " "$idx" "$total" >&2
        _t0=$(date +%s%3N)
        if timeout "$TIMEOUT_SECS" "$fl_bin" -w "$WORKERS" >/dev/null 2>&1; then
            _t1=$(date +%s%3N)
            fl_times+=( $(( _t1 - _t0 )) )
            for f in "$fl_out_hardcoded"/*; do
                [[ -f "$f" ]] && grep -v '^$' "$f" >> "$fl_param_out" || true
            done
        else
            fl_ok=false
            timeout_rows+=( "  param[$idx] ($row): Flowlog timeout/error" )
            > "$fl_param_out"
        fi

        # DuckDB — write SQL to temp file to avoid quoting issues
        local exec_sql="${qwork}/exec_${idx}.sql"
        printf 'SET threads=%s;\n%s\n' "$WORKERS" "$sql_subst" > "$exec_sql"
        printf "\r${CYAN}[CHECK]${NC} DuckDB   [%d/%d]  " "$idx" "$total" >&2
        _t0=$(date +%s%3N)
        if timeout "$TIMEOUT_SECS" "$DUCKDB_BIN" -csv :memory: < "$exec_sql" 2>/dev/null \
            | tail -n +2 > "$db_param_out"; then
            _t1=$(date +%s%3N)
            db_times+=( $(( _t1 - _t0 )) )
        else
            db_ok=false
            timeout_rows+=( "  param[$idx] ($row): DuckDB timeout/error" )
            > "$db_param_out"
        fi

        # Per-param comparison
        if $fl_ok && $db_ok; then
            local cmp
            cmp=$(python3 - "$db_param_out" "$fl_param_out" <<'PYEOF'
import sys, csv
def load_csv(p):
    rows = set()
    with open(p, newline='') as f:
        for r in csv.reader(f):
            if r: rows.add(tuple(r))
    return rows
def load_fl(p):
    rows = set()
    with open(p) as f:
        for line in f:
            line = line.strip()
            if line: rows.add(tuple(line.split('|')))
    return rows
db = load_csv(sys.argv[1]); fl = load_fl(sys.argv[2])
od, of = db - fl, fl - db
if not od and not of:
    print(f"PASS {len(db)}")
else:
    print(f"FAIL db={len(db)} fl={len(fl)} only_db={len(od)} only_fl={len(of)}")
    for r in list(od)[:2]: print(f"    DB: {r}")
    for r in list(of)[:2]: print(f"    FL: {r}")
PYEOF
            )
            if [[ "$cmp" == PASS* ]]; then
                total_rows=$(( total_rows + ${cmp#PASS } ))
            else
                mismatch_rows+=( "  param[$idx] ($row):" )
                while IFS= read -r line; do mismatch_rows+=( "  $line" ); done \
                    <<< "${cmp#FAIL }"
            fi
        fi
    done
    printf "\r%80s\r" "" >&2

    cp "$orig_backup" "$param_file"
    trap - EXIT INT TERM

    # ── Summary ──
    local has_issues=false
    if [[ ${#timeout_rows[@]} -gt 0 || ${#mismatch_rows[@]} -gt 0 ]]; then
        has_issues=true
    fi

    local fl_avg=0 db_avg=0 fl_med=0 db_med=0
    if [[ ${#fl_times[@]} -gt 0 ]]; then
        local fl_sum=0; for t in "${fl_times[@]}"; do fl_sum=$(( fl_sum + t )); done
        fl_avg=$(( fl_sum / ${#fl_times[@]} ))
        fl_med=$(printf '%s\n' "${fl_times[@]}" | sort -n | awk 'BEGIN{c=0}{a[c++]=$1}END{print(c%2?a[int(c/2)]:int((a[c/2-1]+a[c/2])/2))}')
    fi
    if [[ ${#db_times[@]} -gt 0 ]]; then
        local db_sum=0; for t in "${db_times[@]}"; do db_sum=$(( db_sum + t )); done
        db_avg=$(( db_sum / ${#db_times[@]} ))
        db_med=$(printf '%s\n' "${db_times[@]}" | sort -n | awk 'BEGIN{c=0}{a[c++]=$1}END{print(c%2?a[int(c/2)]:int((a[c/2-1]+a[c/2])/2))}')
    fi

    rm -rf "$qwork"

    if ! $has_issues; then
        pass "${query}  (${total_rows} rows, ${total} params)"
        echo "         Flowlog  avg=$(fmt_ms $fl_avg)  median=$(fmt_ms $fl_med)"
        echo "         DuckDB   avg=$(fmt_ms $db_avg)  median=$(fmt_ms $db_med)"
        return 0
    else
        fail "${query}  (${total} params)"
        echo "         Flowlog  avg=$(fmt_ms $fl_avg)  median=$(fmt_ms $fl_med)"
        echo "         DuckDB   avg=$(fmt_ms $db_avg)  median=$(fmt_ms $db_med)"
        if [[ ${#timeout_rows[@]} -gt 0 ]]; then
            echo "         Timeouts (${#timeout_rows[@]}):"
            printf '         %s\n' "${timeout_rows[@]}"
        fi
        if [[ ${#mismatch_rows[@]} -gt 0 ]]; then
            echo "         Mismatches (${#mismatch_rows[@]}):"
            printf '         %s\n' "${mismatch_rows[@]}"
        fi
        return 1
    fi
}


# ── Main loop ─────────────────────────────────────────────────────────────────
total=0; passed=0; failed=0

while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    line="${raw_line%%#*}"
    line="$(trim "$line")"
    [[ -z "$line" ]] && continue

    query="$(trim "${line%%=*}")"
    dataset="$(trim "${line#*=}")"
    [[ -z "$query" || -z "$dataset" ]] && continue

    log "$query (dataset: $dataset)"

    DL_FILE="${DL_DIR}/${query}.dl"
    SQL_FILE="${SQL_DIR}/${query}.sql"

    [[ -f "$DL_FILE" ]]  || { fail "$query: missing $DL_FILE";  failed=$((failed+1)); continue; }
    [[ -f "$SQL_FILE" ]] || { fail "$query: missing $SQL_FILE"; failed=$((failed+1)); continue; }

    total=$((total + 1))

    setup_dataset "$dataset"
    DATA_DIR="${FACT_DIR}/${dataset}"

    if run_per_param "$query" "$DATA_DIR"; then
        passed=$((passed+1))
    else
        failed=$((failed+1))
    fi

    cleanup_dataset "$dataset"

done < "$CONFIG"

echo ""
echo "=========================================="
echo "Results: ${passed}/${total} passed, ${failed} failed"
echo "=========================================="
[[ $failed -eq 0 ]]
