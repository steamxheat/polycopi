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
        return {"ok": True, "status": "dry", "filled": 0.0, "avg_price": None, "error": ""}

    async def close(self):
        pass


class LiveTrader:
    """Настоящие заявки через официальную библиотеку polymarket-client.

    Интерфейс библиотеки вызывается через один метод, чтобы при его изменении
    правка была в одном месте. Перед первым запуском обязательна команда
    `check`: она сверяет, что у установленной версии есть нужные методы.
    """

    mode = "live"

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.client = None
        self._ctx = None

    @staticmethod
    def credentials() -> dict:
        """Ключи читаются из окружения или из ~/.polymarket.env. Значения
        НИКОГДА не логируются и не печатаются."""
        env = {}
        p = Path.home() / ".polymarket.env"
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
        for k in ("POLYMARKET_PRIVATE_KEY", "POLYMARKET_WALLET_ADDRESS"):
            if os.environ.get(k):
                env[k] = os.environ[k]
        return env

    async def start(self):
        from polymarket import AsyncSecureClient            # noqa: PLC0415

        cr = self.credentials()
        if not cr.get("POLYMARKET_PRIVATE_KEY"):
            raise SystemExit("Нет ключа. Положи POLYMARKET_PRIVATE_KEY в ~/.polymarket.env "
                             "(chmod 600). В чат ключ не присылать.")
        kw = {"private_key": cr["POLYMARKET_PRIVATE_KEY"]}
        if cr.get("POLYMARKET_WALLET_ADDRESS"):
            kw["wallet"] = cr["POLYMARKET_WALLET_ADDRESS"]
        self._ctx = await AsyncSecureClient.create(**kw)
        self.client = self._ctx
        log.info(f"Кошелёк подключён: тип {getattr(self.client, 'wallet_type', '?')}")

    async def approvals(self):
        await self.client.setup_trading_approvals()
        log.info("Разрешения на торговлю проверены и выставлены")

    async def buy(self, tok: str, limit: float, shares: float) -> dict:
        """Лимитная заявка на покупку с типом FAK: берём что есть по цене не
        хуже limit, остаток отменяется — висящих заявок не остаётся."""
        try:
            r = await self.client.place_limit_order(
                asset_id=tok, side="BUY", price=f"{limit:.2f}", size=f"{shares:g}",
                order_type=self.cfg["order_type"])
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "status": "error", "filled": 0.0, "avg_price": None,
                    "error": f"{type(e).__name__}: {e}"[:200]}
        ok = bool(getattr(r, "ok", False))
        return {"ok": ok, "status": str(getattr(r, "status", "")),
                "filled": float(getattr(r, "size_matched", 0) or 0),
                "avg_price": getattr(r, "price", None),
                "error": "" if ok else str(getattr(r, "message", ""))[:200]}

    async def close(self):
        if self._ctx is not None and hasattr(self._ctx, "__aexit__"):
            try:
                await self._ctx.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass


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
                 "error": f"{type(e).__name__}: {e}"[:200]}
        finally:
            self.inflight.discard(sig["tok"])
        send_ms = (time.perf_counter() - t0) * 1000
        self.sends.append(send_ms)
        filled = float(r.get("filled") or 0)
        avg = r.get("avg_price")
        try:
            avg = float(avg) if avg not in (None, "") else None
        except (TypeError, ValueError):
            avg = None
        spent = filled * (avg if avg is not None else limit)
        self.spent += spent
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

EXPECTED = ["place_limit_order", "post_order", "setup_trading_approvals"]


def check() -> str:
    L = ["=== Проверка библиотеки polymarket-client ==="]
    try:
        import polymarket                                    # noqa: PLC0415
    except ImportError as e:
        return "\n".join(L + [f"НЕ УСТАНОВЛЕНА: {e}", "",
                              "Поставить:  pip install --break-system-packages polymarket-client",
                              "Старая py-clob-client ЗАБРОШЕНА и не работает — не ставить её."])
    L.append(f"версия: {getattr(polymarket, '__version__', 'неизвестна')}")
    cl = getattr(polymarket, "AsyncSecureClient", None)
    if cl is None:
        return "\n".join(L + ["НЕТ класса AsyncSecureClient — библиотека не та, что ожидается."])
    have = {m: hasattr(cl, m) for m in EXPECTED}
    for m, ok in have.items():
        L.append(f"  {'✅' if ok else '❌'} {m}")
    missing = [m for m, ok in have.items() if not ok]
    L.append("")
    L.append("Все нужные методы на месте — можно запускать dry, потом live."
             if not missing else
             f"ОТСУТСТВУЮТ: {', '.join(missing)}. Живой режим НЕ запускать: "
             f"надо сверить вызовы с документацией установленной версии.")
    cr = LiveTrader.credentials()
    L.append("")
    L.append("ключ найден: " + ("да" if cr.get("POLYMARKET_PRIVATE_KEY") else
                                "НЕТ (~/.polymarket.env или переменная окружения)"))
    L.append("адрес кошелька: " + ("указан" if cr.get("POLYMARKET_WALLET_ADDRESS") else
                                   "не указан (нужен для обычного кошелька)"))
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
        print(check())
        if "ОТСУТСТВУЮТ" in check() or "НЕ УСТАНОВЛЕНА" in check():
            raise SystemExit("Живой режим не запускаю: сначала починить библиотеку (см. выше).")
    try:
        asyncio.run(run(cfg, out, live=a.cmd == "live"))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
