#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run_sweep.sh --mode exits
#   ./run_sweep.sh --mode filters
#   ./run_sweep.sh --mode both
#
# Notes:
# - "exits" sweeps (be_trigger_mult, partial_frac, lock_atr_mult) with fixed entry filters.
# - "filters" sweeps (adx_min, atr_min_pct, breakout_min_delta) with fixed exits.
# - "both" runs exits sweep first, then filters sweep.

MODE="exits"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 --mode exits|filters|both"; exit 0 ;;
    *)
      echo "Unknown arg: $1"; echo "Usage: $0 --mode exits|filters|both"; exit 1 ;;
  esac
done

if [[ "$MODE" != "exits" && "$MODE" != "filters" && "$MODE" != "both" ]]; then
  echo "Invalid --mode: $MODE"; echo "Usage: $0 --mode exits|filters|both"; exit 1
fi

# --- fixed test context ---
SYMBOL="ETHUSDT"
INTERVAL="15m"
START="2026-01-01"
END="2026-02-06"

# --- fixed strategy params (base) ---
EMA_FAST=20
EMA_SLOW=50
EMA_LONG=200
PULLBACK_TOL="0.001"
BRK_LB=20
ATR_P=14
ADX_P=14
SL_ATR=1.0
TP_ATR=2.0
MAX_HOLD=60
SPEND="10"
TAKER_FEE="0.001"
MAKER_FEE="0.0006"

# --- default (fixed) entry filters for exits sweep ---
ADX_MIN_FIXED=15
ATR_MIN_FIXED="0.0015"
BRK_MIN_FIXED="0.0006"

# --- default (fixed) exits for filters sweep ---
BE_FIXED="1.0"
PARTIAL_FIXED="0.25"
LOCK_FIXED="0.4"

# --- sweep params ---
# exits sweep
BE_TRIGGERS=(0.6 0.9 1.0 1.3)
PARTIAL_FRACS=(0.25 0.33)
LOCK_ATR_MULTS=(0.0 0.1 0.2 0.3 0.4)

# filters sweep
ADX_MINS=(15 20 25 30)
ATR_MIN_PCTS=(0.0015 0.0020 0.0025)
BRK_MINS=(0.0006 0.0010 0.0015)

# --- output ---
OUT_DIR="sweeps/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

SUMMARY="$OUT_DIR/summary.tsv"
# Stable header: always include both exit params and filter params.
# For the sweep type that doesn't vary a param, we still record the fixed value.
printf "be_trigger_mult\tpartial_frac\tlock_atr_mult\tadx_min\tatr_min_pct\tbreakout_min_delta\tresult_line\tdiag3_line\treasons_line\n" > "$SUMMARY"

run_case () {
  local be="$1"
  local pf="$2"
  local lock="$3"
  local adx_min="$4"
  local atr_min="$5"
  local brk_min="$6"
  local name="$7"

  local log="$OUT_DIR/${name}.log"

  echo "==> $name"

  python -m src.backtest \
    --symbol "$SYMBOL" --interval "$INTERVAL" \
    --start "$START" --end "$END" \
    --ema_fast "$EMA_FAST" --ema_slow "$EMA_SLOW" --ema_long "$EMA_LONG" \
    --pullback_tol "$PULLBACK_TOL" \
    --breakout_lookback "$BRK_LB" --breakout_min_delta "$brk_min" \
    --atr_period "$ATR_P" --atr_min_pct "$atr_min" \
    --adx_period "$ADX_P" --adx_min "$adx_min" \
    --use_atr_exits \
    --sl_atr_mult "$SL_ATR" \
    --tp_atr_mult "$TP_ATR" \
    --be_trigger_mult "$be" \
    --partial_frac "$pf" \
    --lock_atr_mult "$lock" \
    --max_hold_bars "$MAX_HOLD" \
    --spend "$SPEND" \
    --taker_fee "$TAKER_FEE" \
    --maker_fee "$MAKER_FEE" \
    --tp_as_maker \
    --close_on_end \
    2>&1 | tee "$log" >/dev/null

  # extract key lines (last occurrence)
  local result_line diag3_line reasons_line
  result_line="$(grep -E "RESULT \|" "$log" | tail -n 1 | sed 's/\t/ /g')"
  diag3_line="$(grep -E "DIAG3 \|" "$log" | tail -n 1 | sed 's/\t/ /g')"
  reasons_line="$(grep -E "REASONS \|" "$log" | tail -n 1 | sed 's/\t/ /g')"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$be" "$pf" "$lock" "$adx_min" "$atr_min" "$brk_min" \
    "$result_line" "$diag3_line" "$reasons_line" >> "$SUMMARY"
}

sweep_exits () {
  local adx_min="$ADX_MIN_FIXED"
  local atr_min="$ATR_MIN_FIXED"
  local brk_min="$BRK_MIN_FIXED"

  for be in "${BE_TRIGGERS[@]}"; do
    for pf in "${PARTIAL_FRACS[@]}"; do
      for lock in "${LOCK_ATR_MULTS[@]}"; do
        local name="be${be}_pf${pf}_lock${lock}_adx${adx_min}_brk${brk_min}_atrmin${atr_min}"
        run_case "$be" "$pf" "$lock" "$adx_min" "$atr_min" "$brk_min" "$name"
      done
    done
  done
}

sweep_filters () {
  local be="$BE_FIXED"
  local pf="$PARTIAL_FIXED"
  local lock="$LOCK_FIXED"

  for adx_min in "${ADX_MINS[@]}"; do
    for brk_min in "${BRK_MINS[@]}"; do
      for atr_min in "${ATR_MIN_PCTS[@]}"; do
        local name="adx${adx_min}_brk${brk_min}_atrmin${atr_min}_be${be}_pf${pf}_lock${lock}"
        run_case "$be" "$pf" "$lock" "$adx_min" "$atr_min" "$brk_min" "$name"
      done
    done
  done
}

if [[ "$MODE" == "exits" ]]; then
  sweep_exits
elif [[ "$MODE" == "filters" ]]; then
  sweep_filters
else
  # both
  sweep_exits
  sweep_filters
fi

echo ""
echo "DONE ✅"
echo "Summary: $SUMMARY"
echo "Logs dir: $OUT_DIR"

# Build summary_sorted.tsv (parsed + sorted) and print top rows
python - "$SUMMARY" <<'PY'
import re
import sys
from pathlib import Path

summary = Path(sys.argv[1])
if not summary.exists():
    raise SystemExit(f"Summary file not found: {summary}")

text = summary.read_text(encoding="utf-8", errors="replace").splitlines()
if not text:
    raise SystemExit("Summary is empty")

header = text[0].split("\t")
rows = [line.split("\t") for line in text[1:] if line.strip()]

# Indices
idx = {k: i for i, k in enumerate(header)}
result_i = idx.get("result_line")

re_kv = {
    "pnl_total": re.compile(r"pnl_total=([-+0-9.eE]+)"),
    "trades": re.compile(r"trades=(\d+)"),
    "winrate": re.compile(r"winrate=([0-9.]+%)"),
    "max_dd": re.compile(r"max_dd=([-+0-9.eE]+)"),
    "pf": re.compile(r"pf=([-+0-9.eE]+)"),
}

def grab(rx, s, default=""):
    m = rx.search(s)
    return m.group(1) if m else default

out_header = [
    "pnl_total",
    "be_trigger_mult",
    "partial_frac",
    "lock_atr_mult",
    "adx_min",
    "atr_min_pct",
    "breakout_min_delta",
    "trades",
    "winrate",
    "max_dd",
    "pf",
]

parsed = []
for r in rows:
    result = r[result_i] if result_i is not None and result_i < len(r) else ""
    pnl = grab(re_kv["pnl_total"], result, "")
    trades = grab(re_kv["trades"], result, "")
    winrate = grab(re_kv["winrate"], result, "")
    max_dd = grab(re_kv["max_dd"], result, "")
    pf = grab(re_kv["pf"], result, "")

    def col(name):
        i = idx.get(name)
        return r[i] if i is not None and i < len(r) else ""

    parsed.append({
        "pnl_total": pnl,
        "be_trigger_mult": col("be_trigger_mult"),
        "partial_frac": col("partial_frac"),
        "lock_atr_mult": col("lock_atr_mult"),
        "adx_min": col("adx_min"),
        "atr_min_pct": col("atr_min_pct"),
        "breakout_min_delta": col("breakout_min_delta"),
        "trades": trades,
        "winrate": winrate,
        "max_dd": max_dd,
        "pf": pf,
    })

# sort by pnl_total desc; missing pnl_total -> very low
def as_float(x: str) -> float:
    try:
        return float(x)
    except Exception:
        return float("-inf")

parsed.sort(key=lambda d: as_float(d["pnl_total"]), reverse=True)

out_path = summary.with_name("summary_sorted.tsv")
lines = ["\t".join(out_header)]
for d in parsed:
    lines.append("\t".join(str(d.get(k, "")) for k in out_header))

out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\nWriting summary_sorted.tsv ...")
print(out_path)

print("\nTop by pnl_total:")
print("\n".join(lines[:16]))
PY
