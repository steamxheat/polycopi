#!/usr/bin/env bash
# Короткая сводка по обоим ботам. Запуск на сервере: bash scripts/status.sh
cd "$(dirname "$0")/.."
pm2 jlist 2>/dev/null | python3 -c '
import json,sys,time
for p in json.load(sys.stdin):
    if p["name"] in ("polycopy-sim","polymm-sim"):
        e=p["pm2_env"]; up=(time.time()*1000-e.get("pm_uptime",0))/60000
        print("== %s: %s, работает %.0f мин, перезапусков %s" % (p["name"], e["status"], up, e.get("restart_time",0)))'
echo; echo "===== КОПИ-БОТ ====="
python3 simbot.py report 2>&1 | sed -n '1,14p'
echo; echo "===== МАРКЕТ-МЕЙКЕР ====="
python3 mmsim.py report 2>&1 | head -25
echo; echo "== последние события маркет-мейкера:"
pm2 logs polymm-sim --lines 400 --nostream --raw 2>/dev/null | grep -E "СТАТУС|РЫВОК|ПАРА|ИТОГ|обрыв" | tail -8 | cut -c1-330
echo; echo "== ошибки (если есть):"
pm2 logs polycopy-sim polymm-sim --err --lines 5 --nostream --raw 2>/dev/null | grep -v '^$' | tail -5 || true
