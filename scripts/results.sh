#!/usr/bin/env bash
# Короткая сводка результатов всех ботов на этом сервере.
# Запуск: bash ~/polycopy-sim/scripts/results.sh
cd "$(dirname "$0")/.."
echo "############ $(hostname) — $(date -u '+%Y-%m-%d %H:%M UTC')"
pm2 jlist 2>/dev/null | python3 -c '
import json,sys
ps=[p for p in json.load(sys.stdin) if p["name"].startswith("poly")]
print("боты: " + ", ".join(f"{p[\"name\"]}={p[\"pm2_env\"][\"status\"]}" for p in ps))'
if [ -f data_fv/state.json ]; then
  echo; echo "=== ТЕЙКЕР (polyfv-sim)"
  python3 fvsim.py report 2>/dev/null | sed -n '1,12p' | grep -vE '^\s*$'
fi
for d in data_mm_fast data_mm_eth data_mm_wide10 data_mm_wide16 data_mm_alts; do
  [ -f "$d/state.json" ] || continue
  st=$(pm2 jlist 2>/dev/null | python3 -c "import json,sys; n='polymm-${d#data_mm_}'; print(next((p['pm2_env']['status'] for p in json.load(sys.stdin) if p['name']==n),'нет'))")
  [ "$st" = "online" ] || continue
  echo; echo "=== МЕЙКЕР ${d#data_mm_}"
  python3 mmsim.py report --out "$d" 2>/dev/null | grep -E "^Счёт|^--- |итого|одна нога:" | head -12
done
if [ -f data/state.json ] && pm2 jlist 2>/dev/null | grep -q '"name":"polycopy-sim"'; then
  echo; echo "=== КОПИ-БОТ (polycopy-sim)"
  python3 simbot.py report 2>/dev/null | sed -n '1,8p'
fi
