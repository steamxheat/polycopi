#!/usr/bin/env bash
# Обновить список ботов в шлюзе приложения (polydash-api) под тех, кто сейчас работает, и перезапустить шлюз.
# Запуск: bash ~/polycopy-sim/scripts/dash_bots.sh
set -e
ENV=~/poli-app/server/.env
[ -f "$ENV" ] || { echo "нет $ENV"; exit 1; }
B="polyfv-lag:fv:polycopy-sim/data_fv_lag,polyfv-fav:fv:polycopy-sim/data_fv_fav"
for p in 96 90 84 80 74 68 60; do
  B="$B,polymm-grid:mm:polycopy-sim/data_mmg_$p:p$p"
done
if grep -q '^POLYDASH_BOTS=' "$ENV"; then
  sed -i "s#^POLYDASH_BOTS=.*#POLYDASH_BOTS=$B#" "$ENV"
else
  echo "POLYDASH_BOTS=$B" >> "$ENV"
fi
pm2 restart polydash-api >/dev/null && echo "== шлюз перезапущен, ботов в приложении: $(echo "$B" | tr ',' '\n' | wc -l)"
