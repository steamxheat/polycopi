#!/usr/bin/env bash
# Обновить код с GitHub, прогнать тесты, перезапустить ботов и показать статус.
# Запуск на сервере: bash scripts/update.sh [--reset-mm]
set -e
cd "$(dirname "$0")/.."
echo "== локальные изменения:"; git status --short | grep -v '^??' || echo "(нет)"
git pull --ff-only origin main
echo "== версия: $(git log --oneline -1)"
if ! python3 -m unittest discover -s tests > /tmp/polycopi_tests.log 2>&1; then
  tail -30 /tmp/polycopi_tests.log
  echo "!! ТЕСТЫ НЕ ПРОШЛИ — боты НЕ перезапущены"; exit 1
fi
echo "== тесты: $(grep -E '^Ran' /tmp/polycopi_tests.log) OK"
if [ "$1" = "--reset-mm" ]; then
  pm2 stop polymm-sim >/dev/null
  [ -d data_mm ] && mv data_mm "data_mm_old_$(date +%Y%m%d_%H%M)" && echo "== данные маркет-мейкера отложены"
fi
pm2 restart polycopy-sim polymm-sim --update-env >/dev/null && pm2 save >/dev/null
echo "== перезапущено, жду 90 с..."
sleep 90
bash scripts/status.sh
