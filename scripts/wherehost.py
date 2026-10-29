#!/usr/bin/env python3
"""Где физически стоит сервер биржи — замер из разных городов, бесплатно.

Адрес Polymarket обслуживает Cloudflare по технологии anycast: один и тот же IP
отвечает из сотни точек мира, поэтому геолокация IP бессмысленна (базы дают то
Торонто, то Сан-Франциско). Зато ответы не кэшируются, значит полное время
ответа из города включает дорогу до НАСТОЯЩЕГО сервера. Где быстрее — там он и
стоит.

Нужен, чтобы решить, стоит ли переносить бота: сейчас он в Амстердаме, а
Ирландия и Нидерланды — единственные быстрые страны, где API-торговля разрешена.

Использует бесплатный Globalping (ключ не нужен, лимиты по IP).

  python3 scripts/wherehost.py
  python3 scripts/wherehost.py --target clob.polymarket.com --cities Dublin,Amsterdam
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request

API = "https://api.globalping.io/v1/measurements"
CITIES = ["Dublin", "Amsterdam", "London", "Stockholm", "Frankfurt", "Paris", "Madrid"]
# где API-торговля Polymarket разрешена (официальная документация, кат. «frontend only»
# и страны вне списка ограничений). Нужно, чтобы не гнаться за недоступной скоростью.
ALLOWED = {"Dublin": "✅ Ирландия", "Amsterdam": "✅ Нидерланды", "Stockholm": "✅ Швеция",
           "Madrid": "✅ Испания", "Zurich": "✅ Швейцария", "Barcelona": "✅ Испания",
           "London": "❌ Британия", "Frankfurt": "❌ Германия", "Paris": "❌ Франция",
           "Milan": "❌ Италия", "Warsaw": "❌ Польша"}


def post(url: str, body: dict, timeout: float = 20):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get(url: str, timeout: float = 20):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def start(target: str, cities: list[str], poster=post) -> str:
    body = {"type": "http", "target": target,
            # несколько узлов на город: один может оказаться на плохом канале
            "locations": [{"city": c, "limit": 3} for c in cities],
            "measurementOptions": {"protocol": "HTTPS",
                                   "request": {"path": "/", "method": "GET"}}}
    return poster(API, body)["id"]


def wait(mid: str, getter=get, sleep=time.sleep, tries: int = 20) -> dict:
    for _ in range(tries):
        d = getter(f"{API}/{mid}")
        if d.get("status") != "in-progress":
            return d
        sleep(1.0)
    return d


def rows(d: dict) -> list[tuple]:
    """Город -> лучшее время. ВАЖНО: Globalping отдаёт миллисекунды, не секунды.

    По каждому городу берём МИНИМУМ из нескольких узлов: медленный результат
    почти всегда означает плохой канал у конкретной машины, а нам нужен
    физический предел пути до биржи.
    """
    by_city: dict[str, list] = {}
    fails: dict[str, str] = {}
    for r in d.get("results", []):
        probe = r.get("probe") or {}
        res = r.get("result") or {}
        t = res.get("timings") or {}
        city = probe.get("city") or probe.get("country") or "?"
        total = t.get("total")
        if total is None:
            fails.setdefault(city, res.get("status") or "нет ответа")
            continue
        warm = total - (t.get("dns") or 0) - (t.get("tcp") or 0) - (t.get("tls") or 0)
        by_city.setdefault(city, []).append((total, max(warm, 0.0)))
    out = []
    for city, xs in by_city.items():
        total = min(x[0] for x in xs)
        warm = min(x[1] for x in xs)
        out.append((city, total, warm, len(xs), ""))
    for city, err in fails.items():
        if city not in by_city:
            out.append((city, None, None, 0, err))
    out.sort(key=lambda x: (x[1] is None, x[1]))
    return out


def report(d: dict) -> str:
    rs = rows(d)
    if not rs:
        return "Пусто: замер не вернул результатов."
    L = ["=== Где стоит сервер биржи (замер из городов) ===",
         "Время в МИЛЛИСЕКУНДАХ, по каждому городу — лучший узел из нескольких.",
         "«всего» — весь запрос с нуля; «на живом» — без рукопожатий, как у нашего бота.",
         "Где меньше — там сервер ближе. Торговать можно только из разрешённых стран.", "",
         f"{'город':<14}{'всего':>9}{'на живом':>11}{'узлов':>7}{'API-торговля':>16}"]
    for city, total, warm, n, err in rs:
        if total is None:
            L.append(f"{city:<14}{err:>9}")
            continue
        L.append(f"{city:<14}{total:>8.0f}мс{warm:>10.0f}мс{n:>7}"
                 f"{ALLOWED.get(city, '? проверить'):>16}")
    ok = [(c, w) for c, t, w, _, e in rs if t is not None
          and str(ALLOWED.get(c, "")).startswith("✅")]
    L.append("")
    if ok:
        bc, bw = ok[0]
        ams = next((w for c, w in ok if c == "Amsterdam"), None)
        L.append(f"Лучший из РАЗРЕШЁННЫХ: {bc} — {bw:.0f}мс на живом соединении.")
        if ams is not None and bc != "Amsterdam":
            d_ms = ams - bw
            L.append(f"Наш Амстердам: {ams:.0f}мс. Выигрыш от переезда: {d_ms:.0f}мс.")
            L.append("Меньше 10мс — переезд не стоит возни: по нашей таблице выживания это"
                     if d_ms < 10 else
                     "Больше 10мс — есть смысл проверить на дешёвой машине там же.")
            if d_ms < 10:
                L.append("пара процентов попаданий, не больше.")
        elif ams is not None:
            L.append("Амстердам и так лучший из разрешённых — переезжать некуда.")
    L.append("")
    L.append("Напоминание: быстрые Лондон/Франкфурт/Париж для торговли закрыты,")
    L.append("гнаться за ними бессмысленно — заявки оттуда биржа отвергнет.")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Где стоит сервер биржи: замер из городов")
    ap.add_argument("--target", default="clob.polymarket.com")
    ap.add_argument("--cities", default=",".join(CITIES))
    a = ap.parse_args()
    cities = [c.strip() for c in a.cities.split(",") if c.strip()]
    try:
        mid = start(a.target, cities)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Globalping отказал ({e.code}): {e.read()[:200].decode('utf-8', 'replace')}")
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"Не достучались до Globalping: {type(e).__name__}: {e}")
    print(f"Замер запущен ({', '.join(cities)}), ждём...")
    print(report(wait(mid)))


if __name__ == "__main__":
    main()
