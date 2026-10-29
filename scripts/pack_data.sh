#!/usr/bin/env bash
# Упаковать данные ботов для выгрузки на Mac — без секундных снимков рынка.
#
# snapshots.csv — это гигабайты посекундных слепков стакана, они нужны только
# для backtest.py на самом сервере. Берём то, по чему считаются выводы:
# сделки, итоги окон, состояние и замеры исполнения.
#
#   bash scripts/pack_data.sh            # в ~/polydata.tgz
set -eu
cd "$(dirname "$0")/.."
OUT="${1:-$HOME/polydata.tgz}"

LIST=$(mktemp)
trap 'rm -f "$LIST"' EXIT
for d in data_*/; do
    [ -d "$d" ] || continue
    find "$d" -maxdepth 1 -type f \
        \( -name 'trades*.csv' -o -name 'outcomes*.csv' -o -name 'state.json' \
        -o -name 'survive.csv' -o -name 'levels.csv' -o -name 'orders*.csv' \
        -o -name 'ghosts.csv' -o -name 'episodes*.csv' \) >> "$LIST"
done
# Документы кладём рядом: по ним читаются эти файлы.
find docs -type f -name '*.md' >> "$LIST"

if [ ! -s "$LIST" ]; then
    echo "Нечего упаковывать: папок data_*/ с данными не нашлось." >&2
    exit 1
fi

tar -czf "$OUT" -T "$LIST"
echo "Готово: $OUT"
echo "Размер: $(du -h "$OUT" | cut -f1)"
echo "Файлов: $(wc -l < "$LIST")"
echo
echo "Что внутри:"
while read -r f; do printf '  %-44s %s\n' "$f" "$(du -h "$f" | cut -f1)"; done < "$LIST"
