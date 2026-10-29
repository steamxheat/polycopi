#!/usr/bin/env python3
"""Куда делись деньги. ТОЛЬКО ЧТЕНИЕ — ни одной заявки не отправляет.

Баланс был $10.48, стал $1.93, а наш журнал говорит «потрачено $0.00».
Угадывать нельзя: спрашиваем у самой биржи, какие сделки и позиции она
за нами видит. Если сделок нет — деньги ушли не через торговлю.

  python3 scripts/account.py
"""
from __future__ import annotations

import asyncio
import inspect
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import liveprobe  # noqa: E402
from liveprobe import _num, to_units, usd  # noqa: E402

# Имена методов в библиотеке меняются между версиями, поэтому пробуем
# несколько и честно сообщаем, если ни один не подошёл.
WANTED = {
    "сделки": ("get_trades", "list_trades", "get_user_trades", "trades"),
    "позиции": ("get_positions", "list_positions", "positions"),
    "движения": ("get_activity", "list_activity", "get_transfers", "list_transfers"),
}


async def page_items(v):
    """Отдать записи, что бы метод ни вернул: список, страницу или итератор."""
    if inspect.isawaitable(v):
        v = await v
    if hasattr(v, "first_page"):
        v = await v.first_page()
    items = getattr(v, "items", None)
    if items is not None:
        return list(items)
    if isinstance(v, (list, tuple)):
        return list(v)
    if hasattr(v, "__aiter__"):
        return [x async for x in v]
    return [v] if v is not None else []


def brief(x) -> str:
    """Короткая строка о сделке/позиции: только то, что нужно для сверки."""
    d = x if isinstance(x, dict) else getattr(x, "__dict__", {}) or {}
    keep = ("side", "outcome", "price", "size", "asset_id", "market", "slug",
            "status", "amount", "value", "timestamp", "match_time", "type")
    bits = []
    for k in keep:
        v = d.get(k)
        if v in (None, "", 0):
            continue
        if k in ("amount", "value"):
            v = usd(v)
        bits.append(f"{k}={str(v)[:44]}")
    return "  ".join(bits) or str(x)[:160]


async def main() -> None:
    from polymarket import AsyncSecureClient                     # noqa: PLC0415

    cfg = dict(liveprobe.DEFAULTS)
    t = liveprobe.LiveTrader(cfg)
    cr = t.credentials()
    if not cr.get("POLYMARKET_PRIVATE_KEY"):
        raise SystemExit("Нет POLYMARKET_PRIVATE_KEY в ~/.polymarket.env")
    c = await AsyncSecureClient.create(**t.client_kwargs(cr))

    print("=== Счёт ===")
    print(f"  кошелёк: {getattr(c, 'wallet', '?')}  тип {getattr(c, 'wallet_type', '?')}")
    try:
        b = await c.get_balance_allowance(asset_type="COLLATERAL")
        print(f"  свободные деньги: {usd(getattr(b, 'balance', None))}")
    except Exception as e:                                       # noqa: BLE001
        print(f"  баланс не прочитан: {type(e).__name__}")

    missing = []
    for label, names in WANTED.items():
        fn = next((getattr(c, n) for n in names if hasattr(c, n)), None)
        if fn is None:
            missing.append(label)
            continue
        print(f"\n=== {label} ===")
        try:
            rows = await page_items(fn())
        except Exception as e:                                   # noqa: BLE001
            print(f"  не прочитано: {type(e).__name__}: {str(e)[:140]}")
            continue
        if not rows:
            print("  пусто — биржа за нами такого не числит")
            continue
        total = 0.0
        for r in rows[:30]:
            print(" ", brief(r))
            d = r if isinstance(r, dict) else getattr(r, "__dict__", {}) or {}
            for k in ("value", "amount"):
                v = to_units(_num(d.get(k)), 1e4)
                if v:
                    total += v
                    break
        if len(rows) > 30:
            print(f"  ... всего записей {len(rows)}")
        if total:
            print(f"  на сумму примерно {usd(total)}")

    if missing:
        print(f"\nНе нашёл в библиотеке: {', '.join(missing)}.")
        print("Вот что она умеет (ищи похожее и скажи мне):")
        for n in dir(c):
            if not n.startswith("_") and re.search(
                    r"trade|position|activity|history|portfolio|transfer|value", n, re.I):
                print(f"  {n}")

    for m in ("aclose", "close"):
        f = getattr(c, m, None)
        if f:
            r = f()
            if inspect.isawaitable(r):
                await r
            break


if __name__ == "__main__":
    asyncio.run(main())
