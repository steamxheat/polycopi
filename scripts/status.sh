#!/usr/bin/env bash
# Короткая сводка по ботам. Запуск на сервере: bash scripts/status.sh
cd "$(dirname "$0")/.."
pm2 jlist 2>/dev/null | python3 -c '
import json,sys,time
for p in json.load(sys.stdin):
    if p["name"] in ("polycopy-sim","polymm-sim","polyfv-sim","polymm-t60","polymm-guard","polymm-fast","polymm-eth","polymm-alts","polymm-wide10","polymm-wide16","polymm-wide20","polymm-wide26","polyfv-tune"):
        e=p["pm2_env"]; up=(time.time()*1000-e.get("pm_uptime",0))/60000
        print("== %s: %s, работает %.0f мин, перезапусков %s" % (p["name"], e["status"], up, e.get("restart_time",0)))'
echo; echo "===== КОПИ-БОТ ====="
python3 simbot.py report 2>&1 | awk 'NR>2 && /^--- /{exit} {print}'
echo; echo "===== МАРКЕТ-МЕЙКЕР ====="
python3 mmsim.py report 2>&1 | head -25
echo; echo "== последние события маркет-мейкера:"
pm2 logs polymm-sim --lines 400 --nostream --raw 2>/dev/null | grep -E "СТАТУС|РЫВОК|ПАРА|ИТОГ|обрыв" | tail -8
echo; echo "===== МАРКЕТ-МЕЙКЕР t60 (справедливая цена по модели) ====="
python3 mmsim.py report --out data_mm_t60 2>&1 | head -25
echo; echo "===== МАРКЕТ-МЕЙКЕР guard (стакан + охрана по модели) ====="
python3 mmsim.py report --out data_mm_guard 2>&1 | head -25
echo; echo "===== МАРКЕТ-МЕЙКЕР fast (guard + быстрая реакция) ====="
python3 mmsim.py report --out data_mm_fast 2>&1 | head -25
echo; echo "===== СВОЙ БОТ (справедливая цена) ====="
python3 fvsim.py report 2>&1 | head -12
pm2 logs polyfv-sim --lines 400 --nostream --raw 2>/dev/null | grep -E "СТАТУС|BUY|ИТОГ|обрыв" | tail -6
echo; echo "== ошибки (если есть):"
pm2 logs polycopy-sim polymm-sim polyfv-sim polymm-t60 polymm-guard polymm-fast polymm-eth polymm-alts polymm-wide10 polymm-wide16 --err --lines 5 --nostream --raw 2>/dev/null | grep -v '^$' | tail -5 || true
