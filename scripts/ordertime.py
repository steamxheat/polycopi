#!/usr/bin/env python3
"""Из чего складывается задержка нашей заявки. НИЧЕГО НЕ ОТПРАВЛЯЕТ.

Живой замер показал 750 мс на заявку при 23 мс на чтение. Ждать редких
сигналов, чтобы понять причину, незачем: подпись заявки — локальная операция,
её можно замерить отдельно и сразу.

Скрипт подписывает заявку несколько раз подряд и НЕ отправляет её
(post_order не вызывается — за этим следит тест). Для сравнения замеряет
обычное чтение стакана.

Если подпись занимает сотни миллисекунд — библиотека на каждую заявку ходит
на сервер за данными, и это чинится подготовкой заранее.

  python3 scripts/ordertime.py [--n 5]
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import liveprobe  # noqa: E402
from mmsim import Api  # noqa: E402


async def pick_token(api) -> tuple[str, float] | None:
    """Любой живой рынок BTC/ETH: берём токен и текущий аск."""
    now = api.now()
    for asset in ("btc", "eth"):
        for win, dur in (("5m", 300), ("15m", 900)):
            slug = f"{asset}-updown-{win}-{int(now // dur * dur)}"
            try:
                m = await api.get_market(slug)
            except Exception:  # noqa: BLE001
                continue
            if not m:
                continue
            from core import _as_list                       # noqa: PLC0415
            toks = [str(t) for t in _as_list(m.get("clobTokenIds"))]
            if not toks:
                continue
            try:
                bk = await api.get_book(toks[0])
            except Exception:  # noqa: BLE001
                continue
            from mmsim import best                          # noqa: PLC0415
            a = best(bk, "asks")[0]
            if a and 0.05 < a < 0.95:
                return toks[0], a
    return None


async def post_probe(t, tok: str, ask: float, n: int) -> list[float]:
    """Сколько идёт НАСТОЯЩАЯ отправка заявки бирже.

    Отправляем заявку на покупку по цене 1 цент при рыночной 0.60 и выше:
    исполниться она не может (никто столько не продаёт), но проходит весь
    путь до биржи и обратно. Деньги не тратятся — заявка отвергается как
    «встречных нет», ровно так же, как мы уже видели в живом замере.
    """
    out = []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            signed = await t.client.create_market_order(
                asset_id=tok, side="BUY", amount="1.00", max_price="0.01",
                order_type="FAK")
            t1 = time.perf_counter()
            r = await t.client.post_order(signed)
            ms = (time.perf_counter() - t1) * 1000
            out.append(ms)
            got = liveprobe.parse_reply(r, 0.01, 100)
            print(f"  отправка {i + 1}: {ms:.0f}мс  (подпись {(t1 - t0) * 1000:.0f}мс)"
                  + (f"  исполнено {got['filled']:g} — ПРОВЕРЬ!" if got["filled"] else ""))
        except Exception as e:  # noqa: BLE001
            ms = (time.perf_counter() - t0) * 1000
            out.append(ms)
            print(f"  отправка {i + 1}: {ms:.0f}мс  отказ: {type(e).__name__}: {str(e)[:70]}")
        await asyncio.sleep(0.3)
    return out


async def run(n: int, post: int = 0) -> None:
    cfg = dict(liveprobe.DEFAULTS)
    api = Api(cfg)
    t = liveprobe.LiveTrader(cfg)
    await t.start()

    found = await pick_token(api)
    if not found:
        raise SystemExit("Не нашёл живой рынок с нормальной ценой — попробуй через минуту")
    tok, ask = found
    limit = min(ask + 0.01, 0.95)
    amount = max(round(5 * limit, 2), 1.0)
    print(f"Рынок найден, аск {ask:.2f}, готовим заявку на ${amount:.2f} "
          f"по цене не выше {limit:.2f}\n")

    reads, signs = [], []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            await api.get_book(tok)
            reads.append((time.perf_counter() - t0) * 1000)
        except Exception as e:  # noqa: BLE001
            print(f"  чтение {i + 1}: ошибка {type(e).__name__}")
        t0 = time.perf_counter()
        try:
            await t.client.create_market_order(
                asset_id=tok, side="BUY", amount=f"{amount:.2f}",
                max_price=f"{limit:.2f}", order_type="FAK")
            ms = (time.perf_counter() - t0) * 1000
            signs.append(ms)
            print(f"  подпись {i + 1}: {ms:.0f}мс")
        except Exception as e:  # noqa: BLE001
            print(f"  подпись {i + 1}: ошибка {type(e).__name__}: {str(e)[:120]}")
        await asyncio.sleep(0.2)

    posts = []
    if post:
        print(f"\n--- настоящая отправка, {post} раз (цена 1¢ при рынке {ask:.2f} —")
        print("    исполниться не может, деньги не тратятся) ---")
        posts = await post_probe(t, tok, ask, post)

    print()
    if reads:
        print(f"чтение стакана:  медиана {statistics.median(reads):.0f}мс")
    if signs:
        med = statistics.median(signs)
        print(f"подпись заявки:  медиана {med:.0f}мс  (первая {signs[0]:.0f}, "
              f"последняя {signs[-1]:.0f})")
        print()
        if med > 200:
            print("ПОДПИСЬ ТЯЖЁЛАЯ. Локальная операция столько занимать не может —")
            print("значит библиотека на каждую заявку ходит на сервер. Это чинится:")
            print("нужные данные берём заранее, до появления сигнала.")
        elif signs[0] > med * 3:
            print("Первая подпись намного дольше остальных — библиотека что-то")
            print("кэширует. Достаточно «прогреть» её при запуске.")
        else:
            print("Подпись быстрая — время уходит не на подготовку.")
    if posts:
        pm = statistics.median(posts)
        print(f"отправка заявки: медиана {pm:.0f}мс  (быстрее всех {min(posts):.0f}, "
              f"медленнее всех {max(posts):.0f})")
        print()
        rd = statistics.median(reads) if reads else 0
        print(f"Отправка медленнее чтения в {pm / rd:.0f} раз." if rd else "")
        if pm > 300:
            print("Это и есть наш потолок скорости. Преимущество живёт в окне")
            print("50-100 мс, а заявка идёт втрое-впятеро дольше. Надо либо искать")
            print("другой путь отправки, либо признать, что эта стратегия недоступна.")
        else:
            print("Отправка быстрая — значит те 750 мс в живом замере были не здесь,")
            print("и надо искать причину в другом месте.")
    await t.close()


def main():
    ap = argparse.ArgumentParser(description="Из чего складывается задержка заявки (без отправки)")
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--post", type=int, default=0,
                    help="сколько раз ОТПРАВИТЬ заявку по цене 1¢ (исполниться не может)")
    a = ap.parse_args()
    asyncio.run(run(a.n, a.post))


if __name__ == "__main__":
    main()
