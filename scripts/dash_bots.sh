#!/usr/bin/env bash
# Обновить список ботов в шлюзе приложения (polydash-api) под тех, кто сейчас работает,
# и перезапустить шлюз. Формат записи: имя:тип:путь[:метка]
# Запуск: bash ~/polycopy-sim/scripts/dash_bots.sh
set -e
ENV=~/poli-app/server/.env
[ -f "$ENV" ] || { echo "нет $ENV"; exit 1; }

# Сейчас работают только эти. Мейкерская сетка остановлена 29.10 (минус подтверждён).
# polyprobe — живой замер настоящими заявками; он пишет данные в формате симулятора,
# поэтому читается как обычный бот типа fv. polyfill показать нельзя: у него свой формат.
B="polyfv-lag:fv:polycopy-sim/data_fv_lag"
B="$B,polyfv-fav:fv:polycopy-sim/data_fv_fav"
B="$B,polyprobe:fv:polycopy-sim/data_probe"

if grep -q '^POLYDASH_BOTS=' "$ENV"; then
  sed -i "s#^POLYDASH_BOTS=.*#POLYDASH_BOTS=$B#" "$ENV"
else
  echo "POLYDASH_BOTS=$B" >> "$ENV"
fi
pm2 restart polydash-api >/dev/null
echo "== шлюз перезапущен, ботов в приложении: $(echo "$B" | tr ',' '\n' | wc -l)"
echo "$B" | tr ',' '\n' | sed 's/^/   /'
