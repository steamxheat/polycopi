#!/usr/bin/env bash
# Проверка копи-бота: все ли трейдеры подхватились, идут ли сигналы, нет ли ошибок.
# Запуск на сервере: bash scripts/check_copy.sh
cd "$(dirname "$0")/.."
LOG=$(pm2 logs polycopy-sim --lines 3000 --nostream --raw 2>/dev/null)
python3 - <<'PY'
import pathlib, simbot
c = simbot.load_config(pathlib.Path("config.json"))
extra = pathlib.Path("leaders_extra.json")
print(f"== трейдеров в настройках: {len(c['leaders'])} "
      f"(из разведчика: {len(__import__('json').loads(extra.read_text())) if extra.exists() else 0})")
PY
START=$(echo "$LOG" | grep -n "Старт симуляции" | tail -1 | cut -d: -f1)
AFTER=$(echo "$LOG" | tail -n +"${START:-1}")
echo "== последний запуск: $(echo "$AFTER" | head -1 | cut -c1-160)"
echo "== опрос запущен для: $(echo "$AFTER" | grep -c 'Опрос .*старт') трейдеров"
echo "== ошибки опроса (последние):"
echo "$AFTER" | grep -E "ошибка|обрыв|Traceback|Error" | tail -5 || true
echo "== последний статус по трейдерам:"
echo "$AFTER" | grep -A40 "СТАТУС" | tail -40 | grep -E "СТАТУС|счёт \\$" | tail -27
echo "== последние сделки/пропуски/итоги:"
echo "$AFTER" | grep -E "BUY |SELL |SKIP |ИТОГ" | tail -15
echo "== причины пропусков (за запуск):"
echo "$AFTER" | grep -oE "причина: [a-z_]+" | sort | uniq -c | sort -rn | head
echo "== ошибки pm2:"
pm2 logs polycopy-sim --err --lines 5 --nostream --raw 2>/dev/null | grep -v '^$' | grep -v TAILING | tail -5 || true
