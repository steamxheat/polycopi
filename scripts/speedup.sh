#!/usr/bin/env bash
# Ускорить ботов: поставить orjson (быстрый разбор JSON), перезапустить работающих ботов,
# дать тейкерам приоритет. Запуск: bash ~/polycopy-sim/scripts/speedup.sh
cd "$(dirname "$0")/.."
git pull -q
if ! python3 -c "import orjson" 2>/dev/null; then
  sudo apt-get install -y -qq python3-orjson >/dev/null 2>&1 || {
    sudo apt-get install -y -qq python3-pip >/dev/null 2>&1
    sudo pip3 install -q --break-system-packages orjson
  }
fi
python3 -c "import orjson; print('== orjson', orjson.__version__)" || echo "!! orjson не встал — работаем на обычном json"
RUNNING=$(pm2 jlist 2>/dev/null | python3 -c 'import json,sys; print(" ".join(p["name"] for p in json.load(sys.stdin) if p["name"].startswith(("polymm","polyfv","polyarb")) and p["pm2_env"]["status"]=="online"))')
[ -n "$RUNNING" ] && pm2 restart $RUNNING >/dev/null && pm2 save >/dev/null
echo "== перезапущены: $RUNNING"
sleep 5
PIDS=$(pgrep -f fvsim.py | tr '\n' ' ')
[ -n "$PIDS" ] && sudo renice -n -5 -p $PIDS >/dev/null && echo "== приоритет тейкерам: $PIDS"
sleep 55
uptime
