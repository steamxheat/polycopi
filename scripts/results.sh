#!/usr/bin/env bash
# Короткая сводка результатов всех ботов на этом сервере.
# Запуск: bash ~/polycopy-sim/scripts/results.sh
cd "$(dirname "$0")/.."
echo "############ $(hostname) — $(date -u '+%Y-%m-%d %H:%M UTC')"
pm2 jlist 2>/dev/null | python3 -c '
import json,sys
ps=[p for p in json.load(sys.stdin) if p["name"].startswith("poly")]
print("боты: " + ", ".join(p["name"] + "=" + p["pm2_env"]["status"] for p in ps))'
if [ -f data_fv/state.json ]; then
  echo; echo "=== ТЕЙКЕР (polyfv-sim)"
  python3 fvsim.py report 2>/dev/null | sed -n '1,12p' | grep -vE '^\s*$'
fi
if [ -f data_fv_tune/state.json ]; then
  echo; echo "=== ТЕЙКЕР-ПОДБОР (polyfv-tune): варианты вокруг edge4-early"
  python3 fvsim.py report --out data_fv_tune 2>/dev/null | sed -n '1,16p' | grep -vE '^\s*$'
fi
if pm2 jlist 2>/dev/null | grep -q '"name":"polymm-grid"'; then
  echo; echo "=== МЕЙКЕР-СЕТКА (polymm-grid): все варианты в одном процессе, старт одновременно"
  python3 mmsim.py grid --config mm_grid.json 2>/dev/null
fi
for d in data_mm_fast data_mm_eth data_mm_wide10 data_mm_wide16 data_mm_wide20 data_mm_wide26 data_mm_wide32 data_mm_wide40 data_mm_alts; do
  [ -f "$d/state.json" ] || continue
  st=$(pm2 jlist 2>/dev/null | python3 -c "import json,sys; n='polymm-${d#data_mm_}'; print(next((p['pm2_env']['status'] for p in json.load(sys.stdin) if p['name']==n),'нет'))")
  [ "$st" = "online" ] || continue
  echo; echo "=== МЕЙКЕР ${d#data_mm_}"
  python3 mmsim.py report --out "$d" 2>/dev/null | grep -E "^Счёт|^--- |итого|одна нога:" | head -12
done
if [ -f data_arb/arb.log ]; then
  echo; echo "=== АРБИТРАЖ Up+Down < \$1 (polyarb)"
  python3 arbwatch.py report --out data_arb 2>/dev/null | head -20
  grep СТАТУС data_arb/arb.log | tail -1
fi
if [ -f data/state.json ] && pm2 jlist 2>/dev/null | grep -q '"name":"polycopy-sim"'; then
  echo; echo "=== КОПИ-БОТ (polycopy-sim)"
  python3 simbot.py report 2>/dev/null | sed -n '1,8p'
fi
