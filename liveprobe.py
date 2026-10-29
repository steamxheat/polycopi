#!/usr/bin/env python3
"""Калибровка реальностью: крошечные НАСТОЯЩИЕ заявки, только чтобы замерить.

Это не торговый бот. Он не зарабатывает и не пытается. Он отвечает на два
вопроса, которые бесплатный замер (fillcheck.py) знает, что не может решить:

  1. СКОЛЬКО ЛЕТИТ НАША ЗАЯВКА. fillcheck мерит только чтение (23 мс). Отправка
     идёт другим путём: подпись, доставка, движок сопоставления. От этого
     зависит, в какой строке таблицы выживания мы живём — 85% или 59%.
  2. ДОСТАЮТСЯ ЛИ НАМ ХОРОШИЕ СДЕЛКИ. Симулятор этого знать не может: он не
     встаёт в очередь. Если быстрые боты забирают лучшее, а нам остаётся шлак,
     преимущество исчезает при любой скорости.

Условие входа повторяет lag3-1s-t60. Размер — минимальный, какой разрешает
рынок (5 билетов). Жёсткие пределы: число заявок, общая сумма, цена.

РЕЖИМЫ
  check    — что умеет установленная библиотека (без сети и без ключей)
  dry      — всё как вживую, но заявки НЕ отправляются (ключи не нужны)
  live     — настоящие заявки. Нужны ключи и баланс, см. ТРЕБОВАНИЯ ниже.
  report   — отчёт по уже собранному

ТРЕБОВАНИЯ ДЛЯ live (делает владелец счёта, не бот):
  * торговля на Polymarket должна быть разрешена там, где человек физически;
  * кошелёк и ключ создаёт владелец; ключ лежит ТОЛЬКО на сервере в
    ~/.polymarket.env (chmod 600) и нигде больше;
  * баланс ~$20 в pUSD; один раз вызвать setup_trading_approvals()
    (команда `approvals`);
  * pip install polymarket-client

  python3 liveprobe.py check
  python3 liveprobe.py dry  --out data_probe
  python3 liveprobe.py live --out data_probe --max-orders 40 --max-spend 20
  python3 liveprobe.py report --out data_probe
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import signal
import statistics
import time
from pathlib import Path

import fillcheck
import fvsim
import mmsim
from mmsim import Api, utc

log = mmsim.log
HERE = Path(__file__).resolve().parent

ORDER_COLS = ["time_utc", "mode", "asset", "window", "side", "tok", "ask_seen", "limit",
              "shares", "send_ms", "ok", "status", "filled", "avg_price", "spent",
              "move_bps", "ask_age_s", "error"]

DEFAULTS = dict(fillcheck.DEFAULTS)
DEFAULTS.update({
    "shares": 5.0,              # минимум рынка; меньше нельзя
    "max_orders": 40,           # сколько всего заявок разрешено за запуск
    "max_spend_usd": 20.0,      # сколько всего денег разрешено потратить
    "max_price": 0.95,          # дороже не покупаем никогда
    "limit_extra": 0.01,        # предел цены = увиденный аск + столько (допуск бота)
    "order_type": "FAK",        # взять что можно и отменить остаток; НЕ оставлять заявку
    # (у place_limit_order в 0.14 нет order_type — она была бы GTC и повисла бы)
    "signal_gap_s": 20.0,       # на калибровке спешить некуда, экономим заявки
    "stop_file": "STOP",        # появился файл с таким именем — больше не отправляем
})


# ===================================================================== отправка

class DryTrader:
    """Ничего не отправляет. Нужен, чтобы проверить всю обвязку без денег."""

    mode = "dry"

    async def start(self):
        log.info("СУХОЙ РЕЖИМ: заявки не отправляются, только запись в журнал")

    async def buy(self, tok: str, limit: float, shares: float) -> dict:
        await asyncio.sleep(0)
        return {"ok": True, "status": "dry", "filled": 0.0, "avg_price": None,
                "spent": 0.0, "error": "", "note": ""}

    async def close(self):
        pass


def _num(x):
    """Decimal/строка/None -> float или None."""
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_reply(r, limit: float, shares: float) -> dict:
    """Разбирает AcceptedOrder | RejectedOrder.

    У BUY-заявки taking_amount — полученные билеты, making_amount — потраченные
    деньги. Единицы измерения библиотека может отдавать по-разному, поэтому
    средняя цена считается и проверяется на правдоподобие: если она вне (0, 1],
    считаем её неизвестной и честно говорим об этом, а трату берём по пределу.
    """
    ok = bool(getattr(r, "ok", False))
    filled = _num(getattr(r, "taking_amount", None)) or 0.0
    paid = _num(getattr(r, "making_amount", None))
    avg = paid / filled if (paid and filled > 0) else None
    note = ""
    if avg is not None and not (0.0 < avg <= 1.0):
        note = (f"единицы ответа непонятны: taking={filled:g} making={paid:g} "
                f"-> цена {avg:.4g}, считаю по пределу")
        avg, paid = None, None
    if filled > shares + 1e-9:                     # тоже признак других единиц
        note = note or f"исполнено {filled:g} больше заказанных {shares:g} — проверить единицы"
    spent = paid if paid is not None else filled * limit
    return {"ok": ok, "status": str(getattr(r, "status", "") or getattr(r, "code", "")),
            "filled": filled, "avg_price": avg, "spent": spent,
            "order_id": str(getattr(r, "order_id", "") or ""),
            "error": "" if ok else str(getattr(r, "message", ""))[:200],
            "note": note, "raw_taking": filled, "raw_making": _num(getattr(r, "making_amount", None))}


class LiveTrader:
    """Настоящие заявки через polymarket-client 0.14 (AsyncSecureClient).

    Заявка — РЫНОЧНАЯ с ограничением цены и типом FAK: берём что есть не дороже
    max_price, остаток отменяется. Висящей заявки не остаётся (у лимитной
    заявки в этой версии нет order_type, она была бы GTC — так нельзя).
    """

    mode = "live"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.client = None

    @staticmethod
    def credentials() -> dict:
        """Ключи из окружения или ~/.polymarket.env. Значения НИКОГДА не логируются."""
        env = {}
        p = Path.home() / ".polymarket.env"
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
        for k in ("POLYMARKET_PRIVATE_KEY", "POLYMARKET_WALLET_ADDRESS",
                  "POLYMARKET_RELAYER_API_KEY", "POLYMARKET_RELAYER_API_KEY_ADDRESS",
                  "POLYMARKET_BUILDER_API_KEY", "POLYMARKET_BUILDER_SECRET",
                  "POLYMARKET_BUILDER_PASSPHRASE"):
            if os.environ.get(k):
                env[k] = os.environ[k]
        return env

    @staticmethod
    def client_kwargs(cr: dict):
        """Собирает аргументы клиента из того, что есть.

        Операции без газа требуют ключа Relayer (для существующего счёта) или
        Builder (для создания нового). Одного приватного ключа хватает только
        для EOA-торговли, а она доступна лишь разрешённым адресам.
        """
        from polymarket import AsyncSecureClient                 # noqa: PLC0415,F401

        kw = {"private_key": cr["POLYMARKET_PRIVATE_KEY"]}
        if cr.get("POLYMARKET_WALLET_ADDRESS"):
            kw["wallet"] = cr["POLYMARKET_WALLET_ADDRESS"]
        if cr.get("POLYMARKET_RELAYER_API_KEY"):
            from polymarket import RelayerApiKey                 # noqa: PLC0415
            kw["api_key"] = RelayerApiKey(
                key=cr["POLYMARKET_RELAYER_API_KEY"],
                address=cr.get("POLYMARKET_RELAYER_API_KEY_ADDRESS", ""))
        elif cr.get("POLYMARKET_BUILDER_API_KEY"):
            from polymarket import BuilderApiKey                 # noqa: PLC0415
            kw["api_key"] = BuilderApiKey(
                key=cr["POLYMARKET_BUILDER_API_KEY"],
                secret=cr.get("POLYMARKET_BUILDER_SECRET", ""),
                passphrase=cr.get("POLYMARKET_BUILDER_PASSPHRASE", ""))
            kw.pop("wallet", None)          # Builder сам выводит и разворачивает кошелёк
        return kw

    async def start(self):
        from polymarket import AsyncSecureClient                 # noqa: PLC0415

        cr = self.credentials()
        if not cr.get("POLYMARKET_PRIVATE_KEY"):
            raise SystemExit("Нет ключа. Положи POLYMARKET_PRIVATE_KEY в ~/.polymarket.env "
                             "(chmod 600). В чат ключ не присылать.")
        kw = self.client_kwargs(cr)
        kinds = [k for k in ("api_key", "wallet") if k in kw]
        log.info("Подключаюсь: приватный ключ"
                 + (f" + {', '.join(kinds)}" if kinds else " (без ключа API — только EOA)"))
        self.client = await AsyncSecureClient.create(**kw)
        log.info(f"Кошелёк подключён, тип {getattr(self.client, 'wallet_type', '?')}")
        await self.preflight()

    async def preflight(self):
        """До первой заявки: разрешения, баланс, и снять чужие висящие заявки."""
        try:
            st = await self.client.get_trading_approvals_state()
            log.info(f"Разрешения на торговлю: {st}")
        except Exception as e:  # noqa: BLE001
            log.info(f"Состояние разрешений не прочиталось ({type(e).__name__}); "
                     f"если заявки будут отвергаться — запусти: liveprobe.py approvals")
        try:
            b = await self.client.get_balance_allowance(asset_type="COLLATERAL")
            log.info(f"Баланс/разрешение: {b}")
        except Exception as e:  # noqa: BLE001
            log.info(f"Баланс не прочитался: {type(e).__name__}")
        await self.cancel_all("на старте")

    async def cancel_all(self, when: str):
        try:
            r = await self.client.cancel_all()
            log.info(f"Отмена всех заявок {when}: {r}")
        except Exception as e:  # noqa: BLE001
            log.info(f"Отмена всех заявок {when} не удалась: {type(e).__name__}: {e}")

    async def approvals(self):
        h = await self.client.setup_trading_approvals()
        if hasattr(h, "wait"):
            await h.wait()
        log.info("Разрешения на торговлю выставлены")

    async def buy(self, tok: str, limit: float, shares: float) -> dict:
        try:
            r = await self.client.place_market_order(
                asset_id=tok, side="BUY", shares=f"{shares:g}", max_price=f"{limit:.2f}",
                order_type=self.cfg["order_type"])
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "status": "error", "filled": 0.0, "avg_price": None,
                    "spent": 0.0, "error": f"{type(e).__name__}: {e}"[:200], "note": ""}
        return parse_reply(r, limit, shares)

    async def close(self):
        if self.client is None:
            return
        await self.cancel_all("на выходе")
        for m in ("aclose", "close", "__aexit__"):
            f = getattr(self.client, m, None)
            if f is None:
                continue
            try:
                await (f(None, None, None) if m == "__aexit__" else f())
                break
            except Exception:  # noqa: BLE001
                continue


# ===================================================================== замерщик

class Prober(fillcheck.Watcher):
    """Тот же поиск сигналов, что в fillcheck, плюс настоящая заявка."""

    def __init__(self, cfg: dict, api, out: Path, trader):
        super().__init__(cfg, api, out)
        self.trader = trader
        self.orders = 0
        self.spent = 0.0
        self.sends: list[float] = []
        self.results: list[dict] = []
        self.inflight: set[str] = set()
        self.halted = ""

    def budget_left(self) -> str:
        cfg = self.cfg
        if self.halted:
            return self.halted
        if (self.out / cfg["stop_file"]).exists():
            return f"найден файл {cfg['stop_file']}"
        if self.orders >= cfg["max_orders"]:
            return f"исчерпан лимит заявок ({cfg['max_orders']})"
        if self.spent >= cfg["max_spend_usd"]:
            return f"исчерпан лимит суммы (${cfg['max_spend_usd']:.2f})"
        return ""

    def on_signal(self, sig: dict) -> None:
        stop = self.budget_left()
        if stop:
            if not self.halted:
                self.halted = stop
                self.info(f"СТОП: {stop}. Заявки больше не отправляются, замер продолжается.")
            return
        if sig["tok"] in self.inflight:
            return
        limit = min(sig["ask0"] + self.cfg["limit_extra"], self.cfg["max_price"])
        if limit > self.cfg["max_price"]:
            return
        shares = self.cfg["shares"]
        if self.spent + limit * shares > self.cfg["max_spend_usd"]:
            return
        self.inflight.add(sig["tok"])
        self.orders += 1
        asyncio.ensure_future(self._send(sig, limit, shares))

    async def _send(self, sig: dict, limit: float, shares: float):
        t0 = time.perf_counter()
        try:
            r = await self.trader.buy(sig["tok"], limit, shares)
        except Exception as e:  # noqa: BLE001
            r = {"ok": False, "status": "crash", "filled": 0.0, "avg_price": None,
                 "spent": 0.0, "error": f"{type(e).__name__}: {e}"[:200], "note": ""}
        finally:
            self.inflight.discard(sig["tok"])
        send_ms = (time.perf_counter() - t0) * 1000
        self.sends.append(send_ms)
        filled = float(r.get("filled") or 0)
        avg = r.get("avg_price")
        spent = float(r.get("spent") or 0.0)
        self.spent += spent
        if r.get("note"):
            self.info(f"ВНИМАНИЕ {r['note']}")
        r = {**r, "send_ms": send_ms, "filled": filled, "avg_price": avg, "spent": spent}
        self.results.append(r)
        self.count("order_ok" if r["ok"] else "order_fail")
        self.count("order_filled" if filled > 0 else "order_empty")
        self._csv("orders.csv", ORDER_COLS, {
            "time_utc": utc(sig["t0"]), "mode": self.trader.mode, "asset": sig["mk"].asset,
            "window": sig["mk"].window, "side": sig["side"], "tok": sig["tok"][:12],
            "ask_seen": round(sig["ask0"], 4), "limit": round(limit, 4), "shares": shares,
            "send_ms": round(send_ms), "ok": int(r["ok"]), "status": r.get("status", ""),
            "filled": round(filled, 4), "avg_price": round(avg, 4) if avg is not None else "",
            "spent": round(spent, 4), "move_bps": round(sig["mv"], 2),
            "ask_age_s": round(sig["age"], 2), "error": r.get("error", "")})
        self.info(f"ЗАЯВКА {sig['mk'].asset.upper()} {sig['side']} по {limit:.2f} "
                  f"({shares:g} шт) -> {'взяли' if filled else 'мимо'} {filled:g} за "
                  f"{send_ms:.0f}мс | всего {self.orders}/{self.cfg['max_orders']}, "
                  f"${self.spent:.2f}/${self.cfg['max_spend_usd']:.2f}"
                  + (f" | {r['error']}" if r.get("error") else ""))

    def status(self):
        super().status()
        if self.sends:
            fil = sum(1 for r in self.results if r["filled"] > 0)
            self.info(f"ЗАЯВКИ {self.orders}, исполнено {fil}, потрачено ${self.spent:.2f}, "
                      f"медиана отправки {statistics.median(self.sends):.0f}мс"
                      + (f" | СТОП: {self.halted}" if self.halted else ""))


# ======================================================================= запуск

async def run(cfg: dict, out: Path, live: bool):
    fvsim.setup_logging(out, cfg)
    trader = LiveTrader(cfg) if live else DryTrader()
    await trader.start()
    pr = Prober(cfg, Api(cfg), out, trader)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    pr.info(f"Старт калибровки ({trader.mode}): {cfg['shares']:g} билетов на заявку, "
            f"не больше {cfg['max_orders']} заявок и ${cfg['max_spend_usd']:.2f}. "
            f"Остановить досрочно: touch {out / cfg['stop_file']}")
    loops = [fillcheck.main_loop(pr, stop), fvsim.clob_book_loop(pr, stop),
             mmsim.binance_rest_loop(pr, stop)]
    if cfg["use_coinbase_ws"]:
        loops.append(mmsim.coinbase_loop(pr, stop))
    if cfg["use_okx_ws"]:
        loops.append(mmsim.okx_loop(pr, stop))
    loops += mmsim.extra_feed_loops(pr, cfg, stop)
    tasks = [asyncio.ensure_future(x) for x in loops]
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await trader.close()
    pr.status()
    pr.info(f"Остановлено. Отчёт: python3 liveprobe.py report --out {out}")


async def approvals(cfg: dict, out: Path):
    fvsim.setup_logging(out, cfg, quiet=False)
    t = LiveTrader(cfg)
    await t.start()
    await t.approvals()
    await t.close()


# ======================================================================= check

EXPECTED = ["place_market_order", "post_order", "setup_trading_approvals"]
# «взять что есть и уйти» — без этого заявка повиснет на рынке как GTC
TAKE_HINTS = ("order_type", "fok", "fak", "immediate", "post_only", "expiration",
              "slippage", "max_price", "price")


def _sig(obj, name):
    import inspect                                          # noqa: PLC0415
    try:
        return f"{name}{inspect.signature(getattr(obj, name))}"
    except (TypeError, ValueError, AttributeError):
        return f"{name}(подпись не читается)"


def _fields(cls) -> str:
    mf = getattr(cls, "model_fields", None)                 # pydantic
    if isinstance(mf, dict) and mf:
        return ", ".join(sorted(mf))
    ann = getattr(cls, "__annotations__", {})
    if ann:
        return ", ".join(sorted(ann))
    return ", ".join(n for n in dir(cls) if not n.startswith("_"))[:300]


def check() -> str:
    L = ["=== Разбор установленной библиотеки polymarket-client ==="]
    try:
        import polymarket                                   # noqa: PLC0415
    except ImportError as e:
        return "\n".join(L + [f"НЕ УСТАНОВЛЕНА: {e}", "",
                              "Поставить:  pip install --break-system-packages polymarket-client",
                              "Старая py-clob-client ЗАБРОШЕНА и не работает — не ставить её."])
    L.append(f"версия: {getattr(polymarket, '__version__', 'неизвестна')}")
    cl = getattr(polymarket, "AsyncSecureClient", None)
    if cl is None:
        return "\n".join(L + ["НЕТ класса AsyncSecureClient — библиотека не та, что ожидается."])

    names = [n for n in dir(cl) if not n.startswith("_")]
    orders = sorted(n for n in names if "order" in n.lower())
    L += ["", "--- всё, что про заявки ---"] + [f"  {_sig(cl, n)}" for n in orders]
    other = sorted(n for n in names if n not in orders and
                   any(k in n.lower() for k in ("cancel", "approv", "balance", "book", "trade")))
    if other:
        L += ["", "--- рядом (отмена, разрешения, стакан) ---"] + \
             [f"  {_sig(cl, n)}" for n in other]

    L += ["", "--- поля ответов ---"]
    for mod_name in ("models.clob.order_response", "models"):
        try:
            mod = __import__(f"polymarket.{mod_name}", fromlist=["*"])
        except ImportError:
            continue
        for cname in ("AcceptedOrder", "RejectedOrder", "OrderResponse"):
            c = getattr(mod, cname, None)
            if c is not None:
                L.append(f"  {cname}: {_fields(c)}")
        break

    L += ["", "--- можно ли НЕ оставлять заявку висеть ---"]
    hints = []
    for n in orders:
        sig = _sig(cl, n).lower()
        found = [h for h in TAKE_HINTS if h in sig]
        if found:
            hints.append(f"  {n}: {', '.join(found)}")
    L += hints or ["  подходящих признаков не найдено"]

    missing = [m for m in EXPECTED if not hasattr(cl, m)]
    L.append("")
    L.append(f"ЖИВОЙ РЕЖИМ НЕ ЗАПУСКАТЬ: отсутствуют {', '.join(missing)}" if missing else
             "Базовые методы на месте. Вызовы в коде надо сверить с подписями выше.")
    cr = LiveTrader.credentials()
    L += ["", "--- что есть в ~/.polymarket.env (значения не показываю) ---"]
    have = {
        "приватный ключ подписи": "POLYMARKET_PRIVATE_KEY",
        "адрес кошелька": "POLYMARKET_WALLET_ADDRESS",
        "ключ Relayer": "POLYMARKET_RELAYER_API_KEY",
        "адрес ключа Relayer": "POLYMARKET_RELAYER_API_KEY_ADDRESS",
        "ключ Builder": "POLYMARKET_BUILDER_API_KEY",
    }
    for label, key in have.items():
        L.append(f"  {'✅' if cr.get(key) else '—'} {label}")
    if not cr.get("POLYMARKET_PRIVATE_KEY"):
        L += ["", "Без приватного ключа живой режим не запустится."]
    elif cr.get("POLYMARKET_RELAYER_API_KEY") or cr.get("POLYMARKET_BUILDER_API_KEY"):
        L += ["", "Набор полный: торговля без газа будет работать."]
    else:
        L += ["", "Есть только приватный ключ. Это путь EOA — он работает лишь для",
              "разрешённых адресов и требует POL на газ. Для обычного счёта нужен",
              "ключ Relayer с polymarket.com (профиль -> API/разработчику)."]
    return "\n".join(L)


# ======================================================================= отчёт

def report(out: Path) -> str:
    rows = fillcheck.rows(out / "orders.csv")
    if not rows:
        return f"Нет заявок в {out}. Сначала: python3 liveprobe.py dry --out {out}"

    def f(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    live = [r for r in rows if r.get("mode") == "live"]
    use = live or rows
    L = [f"=== Калибровка реальностью: {out} ===",
         f"заявок: {len(rows)} (настоящих: {len(live)}, сухих: {len(rows) - len(live)})", ""]
    if not live:
        L.append("Пока только сухой режим — проверялась обвязка, заявки не уходили.")
        L.append("")
    sends = [x for x in (f(r["send_ms"]) for r in use) if x is not None]
    if sends:
        L += ["1) СКОЛЬКО ЛЕТИТ НАША ЗАЯВКА",
              f"   медиана {statistics.median(sends):.0f}мс, 25% быстрее "
              f"{fillcheck.pct(sends, 25):.0f}мс, 10% медленнее "
              f"{fillcheck.pct(sends, 90):.0f}мс",
              "   -> смотри эту задержку в таблице выживания fillcheck: она говорит,",
              "      какую долю попаданий нам вообще отводит рынок.", ""]
    if live:
        okn = sum(r["ok"] == "1" for r in live)
        fil = [r for r in live if (f(r["filled"]) or 0) > 0]
        full = [r for r in fil if (f(r["filled"]) or 0) >= (f(r["shares"]) or 1e9) - 1e-9]
        L += ["2) ЧТО ИЗ ЭТОГО ИСПОЛНИЛОСЬ",
              f"   принято биржей: {okn}/{len(live)}",
              f"   исполнено хоть частично: {len(fil)}/{len(live)} = {len(fil) / len(live):.0%}",
              f"   исполнено полностью: {len(full)}/{len(live)} = {len(full) / len(live):.0%}"]
        slip = [(f(r["avg_price"]) or 0) - (f(r["ask_seen"]) or 0) for r in fil
                if f(r["avg_price"]) is not None]
        if slip:
            L.append(f"   цена против увиденной: медиана {statistics.median(slip) * 100:+.2f}¢")
        spent = sum(f(r["spent"]) or 0 for r in live)
        L.append(f"   потрачено всего: ${spent:.2f}")
        errs: dict[str, int] = {}
        for r in live:
            if r.get("error"):
                errs[r["error"][:60]] = errs.get(r["error"][:60], 0) + 1
        if errs:
            L += ["", "   отказы:"] + [f"     {n}x  {e}" for e, n in
                                       sorted(errs.items(), key=lambda x: -x[1])[:5]]
        L += ["", "3) ДОСТАЮТСЯ ЛИ НАМ ХОРОШИЕ СДЕЛКИ",
              "   Сравни долю исполнения выше с предсказанием fillcheck на нашей задержке.",
              "   Исполнение заметно НИЖЕ предсказания — лучшее забирают до нас (отбор худшего).",
              "   Примерно совпадает — отбора нет, и результат симулятора надо просто",
              "   умножить на эту долю."]
    L += ["", f"Итог по деньгам считается после расчёта окон: python3 fillcheck.py report --out {out}"]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Калибровка исполнения настоящими крошечными заявками")
    ap.add_argument("cmd", choices=("check", "dry", "live", "approvals", "report"))
    ap.add_argument("--out", default=str(HERE / "data_probe"))
    ap.add_argument("--config", default=str(HERE / "probe_config.json"))
    ap.add_argument("--max-orders", type=int)
    ap.add_argument("--max-spend", type=float)
    ap.add_argument("--shares", type=float)
    a = ap.parse_args()
    out = Path(a.out)
    if a.cmd == "check":
        print(check())
        return
    if a.cmd == "report":
        print(report(out))
        return
    cfg = dict(DEFAULTS)
    cp = Path(a.config)
    if cp.exists():
        cfg.update(json.loads(cp.read_text()))
    for k, v in (("max_orders", a.max_orders), ("max_spend_usd", a.max_spend),
                 ("shares", a.shares)):
        if v is not None:
            cfg[k] = v
    out.mkdir(parents=True, exist_ok=True)
    if a.cmd == "approvals":
        asyncio.run(approvals(cfg, out))
        return
    if a.cmd == "live":
        ch = check()
        print(ch)
        if "НЕ ЗАПУСКАТЬ" in ch or "НЕ УСТАНОВЛЕНА" in ch:
            raise SystemExit("Живой режим не запускаю: сначала починить библиотеку (см. выше).")
    try:
        asyncio.run(run(cfg, out, live=a.cmd == "live"))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
