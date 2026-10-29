#!/usr/bin/env python3
"""Замер реальности: успели бы мы забрать билет — БЕЗ ДЕНЕГ И БЕЗ КЛЮЧЕЙ.

Симулятор тейкера покупает по списку заявок, который сам запоздал, и считает, что
попал всегда. Этот модуль ничего не покупает, он только измеряет три вещи:

  1. СКОЛЬКО ЖИВЁТ ЛУЧШАЯ ЗАЯВКА. По живой ленте CLOB: появился лучший аск — ждём,
     когда он исчезнет. Если такие заявки живут 2 секунды, мы успеваем. Если 80 мс —
     не успеваем никогда, и реальные деньги уже не нужны.

  2. ВЫЖИВАЕТ ЛИ ЦЕНА ПОСЛЕ РЫВКА. В момент, когда наш бот захотел бы купить
     (быстрые фьючерсы дёрнулись, а аск Polymarket ещё не переставлен), запоминаем
     цену и проверяем через 50/100/200/500/1000 мс: она ещё на месте? Это и есть
     «доля исполнения» при разной задержке нашей заявки.

  3. ПРИЗРАКИ. Сравниваем запоздавший список заявок (REST, по нему решает симулятор)
     с живой лентой. Если REST показывает билет дешевле, чем есть в живом стакане —
     это призрак: симулятор запишет прибыль со сделки, которой не было.

Запуск:  python3 fillcheck.py run --out data_fill
Отчёт:   python3 fillcheck.py report --out data_fill
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import signal
import statistics
import time
from collections import defaultdict, deque
from pathlib import Path

import fvsim
import mmsim
from core import _as_list, walk_book
from mmsim import Api, PriceFeed, best, utc

log = mmsim.log            # тот же журнал, что у остальных ботов (polymm)
HERE = Path(__file__).resolve().parent

HORIZONS_MS = (50, 100, 200, 500, 1000)

DEFAULTS = {
    "assets": {"btc": "BTCUSDT", "eth": "ETHUSDT"},
    "windows": {"5m": 300, "15m": 900},
    # условие, при котором наш бот хочет купить (как у lag3-1s-t60)
    "move_bps": 3.0,            # рывок на быстрых фьючерсах
    "move_s": 1.0,              # за столько секунд
    "ask_stale_s": 1.0,         # аск Polymarket не менялся столько секунд
    "min_price": 0.05,
    "max_price": 0.95,
    "signal_gap_s": 5.0,        # не брать один и тот же токен чаще, чем раз в N с
    "fill_tolerance": 0.01,     # бот готов взять до «увиденная цена + 1¢» — как в fvsim
    "rest_every_s": 2.0,        # как часто сверять REST с живой лентой
    "measure": True,            # вести замеры рынка (время жизни заявок, выживание, призраки)
    "loop_s": 0.02,
    "trigger_gap_s": 0.01,      # не чаще раза в 10 мс (цены приходят пачками)
    "status_every_s": 60,
    "min_tau": 8,
    "ws_silence_s": 10,
    "log_max_mb": 20, "log_backups": 5,
    "clob_url": "https://clob.polymarket.com",
    "gamma_url": "https://gamma-api.polymarket.com",
    "clob_ws": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "binance_rest": "https://data-api.binance.vision",
    "binance_ws": "wss://data-stream.binance.vision/stream",
    "coinbase_ws": "wss://ws-feed.exchange.coinbase.com",
    "okx_ws": "wss://ws.okx.com:8443/ws/v5/public",
    "bybit_perp_ws": "wss://stream.bybit.com/v5/public/linear",
    "use_binance_ws": False, "use_coinbase_ws": True, "use_okx_ws": True,
    "use_bybit_perp_ws": True, "use_okx_perp_ws": True,
    "lead_sources": ["bybit_perp", "okx_perp", "coinbase_ws"],
    "lead_agree": 1,
}

LEVEL_COLS = ["time_utc", "asset", "window", "tok", "price", "life_ms", "move_bps", "after_move"]
SURV_COLS = ["time_utc", "asset", "window", "tok", "side", "ask0", "move_bps", "ask_age_s"] + \
            [f"ok_{t}" for t in HORIZONS_MS] + [f"okt_{t}" for t in HORIZONS_MS]
GHOST_COLS = ["time_utc", "asset", "tok", "rest_ask", "ws_ask", "diff_c",
              "rest_age_ms", "call_ms", "ghost",
              # сколько денег реально можно вложить в одну сделку (потолок по объёму)
              "usd_at_best", "usd_within_1c", "usd_within_3c", "vwap_100usd"]


def load_config(path: Path) -> dict:
    cfg = dict(DEFAULTS)
    if path.exists():
        cfg.update(json.loads(path.read_text()))
    return cfg


class TrackedBook(fvsim.Book):
    """Книга из живой ленты + уведомление, когда лучший аск сменился."""

    def __init__(self, on_change):
        super().__init__()
        self._on_change = on_change

    def _set(self, tok, b, a, now):
        prev = self.ask.get(tok)
        super()._set(tok, b, a, now)
        new = self.ask.get(tok)
        if new != prev:
            self._on_change(tok, prev, new, now)


class Watcher:
    def __init__(self, cfg: dict, api, out: Path, quiet: bool = False):
        self.cfg, self.api, self.out, self.quiet = cfg, api, out, quiet
        out.mkdir(parents=True, exist_ok=True)
        self.markets: dict[str, fvsim.Mkt] = {}
        self.tok_mkt: dict[str, fvsim.Mkt] = {}
        self.book = TrackedBook(self.on_ask_change)
        self.prices = PriceFeed(list(cfg["assets"].values()), keep_s=60)
        # как у бота: решаем на КАЖДОЙ новой цене, иначе короткие рывки проходят мимо
        self._last_trig = 0.0
        self.prices.on_add = self._on_price
        self.counters: dict[str, int] = defaultdict(int)
        self.ws_last = 0.0
        self.open_level: dict[str, tuple] = {}        # tok -> (цена, когда появилась, рывок)
        # ВАЖНО: всё измеренное пишется в CSV, в памяти держим только последнее окно
        # для строки статуса — иначе за ночь это сотни тысяч записей и утечка.
        self.lives: deque = deque(maxlen=20000)       # (life_ms, after_move)
        self.n_lives = 0                              # точный счётчик, без хранения
        self.pending: deque = deque()                 # проверки выживания цены
        self.surv: deque = deque(maxlen=5000)         # (ask0, [ok по горизонтам])
        self.n_surv = 0
        self.last_signal: dict[str, float] = {}
        self.rtt: deque = deque(maxlen=5000)
        self.rest_age: deque = deque(maxlen=5000)
        self.ghost_n = self.rest_n = 0
        self.started = self.api.now()
        self._rr = 0
        self._csv_checked: set[str] = set()

    # ------------------------------------------------------------------ служебное
    def count(self, k, n=1):
        self.counters[k] += n

    def info(self, m):
        log.info(m)

    def _csv(self, name, cols, row):
        p = self.out / name
        if name not in self._csv_checked:
            self._csv_checked.add(name)
            if p.exists():
                with p.open() as f:
                    if f.readline().strip().split(",") != cols:
                        p.replace(p.with_name(f"{p.stem}_old_{int(time.time())}{p.suffix}"))
        new = not p.exists()
        with p.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            if new:
                w.writeheader()
            w.writerow({c: row.get(c, "") for c in cols})

    def rets(self, asset: str) -> list:
        """Движения цены по быстрым источникам за move_s секунд — как в fvsim."""
        sym = self.cfg["assets"].get(asset)
        if not sym:
            return []
        return self.prices.rets_bps(sym, self.api.now(), self.cfg["move_s"],
                                    self.cfg.get("lead_sources"))

    def move_bps(self, asset: str) -> float:
        """Самое сильное движение по модулю — только чтобы помечать уровни."""
        rs = self.rets(asset)
        return max(rs, key=abs) if rs else 0.0

    def moved(self, asset: str, side: str, rets=None) -> bool:
        """Сдвинулась ли цена В СТОРОНУ side настолько, что бот захотел бы купить.
        rets можно передать готовыми: считать их на каждую сторону — впустую жечь процессор."""
        sgn = 1 if side == "Up" else -1
        need = self.cfg.get("lead_agree", 1)
        rs = self.rets(asset) if rets is None else rets
        return sum(1 for x in rs if sgn * x >= self.cfg["move_bps"]) >= need

    def tick_second(self) -> None:
        """Раз в секунду. В замере без денег не нужна, liveprobe считает здесь модель."""

    async def resolve_markets(self) -> None:
        """Чем кончились окна. Без денег не нужно, liveprobe считает здесь прибыль."""

    def _on_price(self, symbol: str, ts: float) -> None:
        """Новая цена с быстрой биржи — сразу проверяем условие и горизонты."""
        now = self.api.now()
        if now - self._last_trig < self.cfg["trigger_gap_s"]:
            return
        self._last_trig = now
        self.count("fast_trigger")
        try:
            self.check_signals()
            self.resolve_pending()
        except Exception as e:  # noqa: BLE001
            log.debug(f"быстрая проверка: {e}")

    # ------------------------------------------------- 1) время жизни лучшего аска
    def on_ask_change(self, tok, prev, new, now):
        if not self.cfg.get("measure", True):
            return                       # в живом режиме это лишняя работа в цикле
        mk = self.tok_mkt.get(tok)
        old = self.open_level.pop(tok, None)
        if old and prev is not None:
            price, t0, mv = old
            life_ms = (now - t0) * 1000
            after = abs(mv) >= self.cfg["move_bps"]
            if life_ms >= 0:
                self.lives.append((life_ms, after))
                self.n_lives += 1
                self.count("levels")
                self._csv("levels.csv", LEVEL_COLS, {
                    "time_utc": utc(t0), "asset": mk.asset if mk else "", "tok": tok[:12],
                    "window": mk.window if mk else "", "price": round(price, 4),
                    "life_ms": round(life_ms), "move_bps": round(mv, 2), "after_move": int(after)})
        if new is not None and mk is not None:
            self.open_level[tok] = (new, now, self.move_bps(mk.asset))

    # ------------------------------------------- 2) выживание цены после сигнала
    def check_signals(self):
        now = self.api.now()
        cfg = self.cfg
        rets_by_asset: dict[str, list] = {}
        for mk in self.markets.values():
            tau = mk.end - now
            if tau < cfg["min_tau"] or now < mk.start:
                continue
            if mk.asset not in rets_by_asset:
                rets_by_asset[mk.asset] = self.rets(mk.asset)
            rs = rets_by_asset[mk.asset]
            if not rs:
                continue
            mv_all = max(rs, key=abs)
            for side, tok in (("Up", mk.up), ("Down", mk.down)):
                if not self.moved(mk.asset, side, rs):
                    continue
                mv = mv_all
                a = self.book.ask.get(tok)
                if a is None or not (cfg["min_price"] <= a <= cfg["max_price"]):
                    continue
                age = now - self.book.ask_t.get(tok, now)
                if age < cfg["ask_stale_s"]:
                    continue                       # аск только что переставили — не наш случай
                if now - self.last_signal.get(tok, 0) < cfg["signal_gap_s"]:
                    continue
                self.last_signal[tok] = now
                sig = {"tok": tok, "ask0": a, "t0": now, "mv": mv, "age": age,
                       "mk": mk, "side": side, "ok": {}, "okt": {}}
                if self.cfg.get("measure", True):
                    self.pending.append(sig)
                self.count("signals")
                self.on_signal(sig)

    def on_signal(self, sig: dict) -> None:
        """Точка подключения: liveprobe отправляет здесь настоящую заявку.
        В замере без денег не делает ничего."""

    def resolve_pending(self):
        """Горизонты считаем ПО ИСТОРИИ аска, а не в момент опроса: иначе
        задержка самого опроса приписывается цене и занижает наши шансы."""
        now = self.api.now()
        while self.pending and (now - self.pending[0]["t0"]) * 1000 >= HORIZONS_MS[-1]:
            self._finish(self.pending.popleft())

    def _finish(self, p):
        tol = self.cfg["fill_tolerance"]
        for t in HORIZONS_MS:
            a = self.book.ask_at(p["tok"], p["t0"] + t / 1000)
            p["ok"][t] = int(a is not None and a <= p["ask0"] + 1e-9)
            p["okt"][t] = int(a is not None and a <= p["ask0"] + tol + 1e-9)
        self.surv.append((p["ask0"], [p["ok"][t] for t in HORIZONS_MS]))
        self.n_surv += 1
        row = {"time_utc": utc(p["t0"]), "asset": p["mk"].asset, "window": p["mk"].window,
               "tok": p["tok"][:12], "side": p["side"], "ask0": round(p["ask0"], 4),
               "move_bps": round(p["mv"], 2), "ask_age_s": round(p["age"], 2)}
        row.update({f"ok_{t}": p["ok"][t] for t in HORIZONS_MS})
        row.update({f"okt_{t}": p["okt"][t] for t in HORIZONS_MS})
        self._csv("survive.csv", SURV_COLS, row)

    # ------------------------------------------------------ 3) призраки в REST
    async def check_rest(self):
        if not self.cfg.get("measure", True):
            return
        toks = [t for m in self.markets.values() if not m.resolved for t in (m.up, m.down)
                if t in self.book.ask]
        if not toks:
            return
        tok = toks[self._rr % len(toks)]
        self._rr += 1
        t0 = time.perf_counter()
        try:
            bk = await self.api.get_book(tok)
        except Exception as e:  # noqa: BLE001
            self.count("rest_error")
            log.debug(f"book: {e}")
            return
        call_ms = (time.perf_counter() - t0) * 1000
        self.rtt.append(call_ms)
        ra = best(bk, "asks")[0]
        wa = self.book.ask.get(tok)
        try:
            age = self.api.now() * 1000 - float(bk.get("timestamp"))
        except (TypeError, ValueError):
            age = ""
        if isinstance(age, float):
            self.rest_age.append(age)
        if ra is None or wa is None:
            return
        self.rest_n += 1
        ghost = ra < wa - 1e-9              # REST обещает дешевле, чем есть в живом стакане
        self.ghost_n += ghost
        mk = self.tok_mkt.get(tok)
        cap = self.depth(bk, ra)
        self._csv("ghosts.csv", GHOST_COLS, {
            "time_utc": utc(self.api.now()), "asset": mk.asset if mk else "", "tok": tok[:12],
            "rest_ask": round(ra, 4), "ws_ask": round(wa, 4), "diff_c": round((wa - ra) * 100, 2),
            "rest_age_ms": round(age) if isinstance(age, float) else "",
            "call_ms": round(call_ms), "ghost": int(ghost), **cap})

    def depth(self, bk: dict, ask: float) -> dict:
        """Потолок по объёму: сколько долларов можно взять по лучшей цене и около неё,
        и во сколько обойдётся попытка вложить $100 (средняя цена поедет вверх)."""
        out = {}
        for name, lim in (("usd_at_best", ask), ("usd_within_1c", ask + 0.01),
                          ("usd_within_3c", ask + 0.03)):
            f = walk_book(bk, "BUY", 1e9, lim, 0.0)
            out[name] = round(f.notional, 2)
        f = walk_book(bk, "BUY", 100.0 / max(ask, 0.01), 1.0, 0.0)
        out["vwap_100usd"] = round(f.vwap, 4) if f.shares > 0 else ""
        return out

    # ------------------------------------------------------------------ рынки
    async def discover(self):
        now = self.api.now()
        for asset in self.cfg["assets"]:
            for win, dur in self.cfg["windows"].items():
                start = int(now // dur * dur)
                for st in (start, start + dur):
                    slug = f"{asset}-updown-{win}-{st}"
                    if slug in self.markets or st > now + 20:
                        continue
                    try:
                        m = await self.api.get_market(slug)
                    except Exception:  # noqa: BLE001
                        continue
                    if not m:
                        continue
                    toks = [str(t) for t in _as_list(m.get("clobTokenIds"))]
                    outs = [str(o).lower() for o in _as_list(m.get("outcomes"))]
                    if len(toks) != 2 or outs != ["up", "down"]:
                        continue
                    mk = fvsim.Mkt(slug, asset, win, st, st + dur, toks[0], toks[1],
                                   str(m.get("conditionId", "")))
                    self.markets[slug] = mk
                    self.tok_mkt[toks[0]] = self.tok_mkt[toks[1]] = mk
        # 15 минут, а не 2: итоги у Polymarket появляются не сразу, а по ним
        # считается прибыль купленных билетов — потеряем окно, потеряем позицию
        for slug in [s for s, m in self.markets.items() if m.end < now - 900]:
            m = self.markets.pop(slug)
            for t in (m.up, m.down):
                self.tok_mkt.pop(t, None)
                self.open_level.pop(t, None)
                self.last_signal.pop(t, None)
            m.resolved = True

    def status(self):
        srcs = ",".join(self.prices.sources_alive(self.api.now())) or "НЕТ ЦЕН"
        med = f"{statistics.median(x for x, _ in self.lives):.0f}мс" if self.lives else "-"
        ok = ""
        if self.surv:
            i = HORIZONS_MS.index(200)
            ok = f" | цена жива через 200мс: {sum(s[1][i] for s in self.surv) / len(self.surv):.0%}"
        g = f" | призраков {self.ghost_n}/{self.rest_n}" if self.rest_n else ""
        self.info(f"ЗАМЕР рынков {len(self.markets)} | цены: {srcs} | уровней {self.n_lives} "
                  f"(медиана жизни {med}) | сигналов {self.n_surv}{ok}{g}")


# ======================================================================= циклы

async def main_loop(w: Watcher, stop: asyncio.Event):
    last_disc = last_rest = last_status = last_tick = last_res = 0.0
    while not stop.is_set():
        now = time.time()
        try:
            if now - last_tick >= 1.0:
                w.tick_second()
                last_tick = now
            if now - last_disc >= 5:
                await w.discover()
                last_disc = now
            w.check_signals()
            w.resolve_pending()
            if now - last_rest >= w.cfg["rest_every_s"]:
                await w.check_rest()
                last_rest = now
            if now - last_res >= 10:
                await w.resolve_markets()
                last_res = now
        except Exception as e:  # noqa: BLE001
            log.exception(f"loop: {e}")
        if now - last_status >= w.cfg["status_every_s"]:
            try:
                w.status()
            except Exception as e:  # noqa: BLE001
                log.debug(f"status: {e}")
            last_status = now
        try:
            await asyncio.wait_for(stop.wait(), w.cfg["loop_s"])
        except asyncio.TimeoutError:
            pass


async def run(cfg: dict, out: Path):
    fvsim.setup_logging(out, cfg)
    w = Watcher(cfg, Api(cfg), out)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass
    w.info(f"Старт замера исполнения (без денег): рывок ≥{cfg['move_bps']} б.п. за "
           f"{cfg['move_s']}с, аск не моложе {cfg['ask_stale_s']}с. Горизонты: "
           + ", ".join(f"{t}мс" for t in HORIZONS_MS))
    loops = [main_loop(w, stop), fvsim.clob_book_loop(w, stop), mmsim.binance_rest_loop(w, stop)]
    if cfg["use_coinbase_ws"]:
        loops.append(mmsim.coinbase_loop(w, stop))
    if cfg["use_okx_ws"]:
        loops.append(mmsim.okx_loop(w, stop))
    loops += mmsim.extra_feed_loops(w, cfg, stop)
    tasks = [asyncio.ensure_future(x) for x in loops]
    await stop.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    w.status()
    w.info("Остановлено. Отчёт: python3 fillcheck.py report --out " + str(out))


# ======================================================================= отчёт

def pct(xs, q):
    xs = sorted(xs)
    if not xs:
        return 0.0
    i = min(len(xs) - 1, max(0, int(q / 100 * (len(xs) - 1))))
    return xs[i]


def rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def report(out: Path) -> str:
    lv = rows(out / "levels.csv")
    sv = rows(out / "survive.csv")
    gh = rows(out / "ghosts.csv")
    if not (lv or sv or gh):
        return f"Нет данных в {out}. Сначала: python3 fillcheck.py run --out {out}"
    L = [f"=== Замер исполнения (без денег): {out} ===", ""]

    def f(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    # 1) время жизни лучшей заявки
    lives = [(f(r["life_ms"]), r["after_move"] == "1") for r in lv]
    lives = [(x, a) for x, a in lives if x is not None]
    L += ["1) СКОЛЬКО ЖИВЁТ ЛУЧШАЯ ЗАЯВКА (от появления до исчезновения)",
          "   Если заявки живут дольше нашей задержки — мы успеваем их забрать.", ""]
    if lives:
        L.append(f"{'выборка':<22}{'шт':>7}{'медиана':>10}{'25%':>9}{'10%':>9}")
        for name, xs in (("все заявки", [x for x, _ in lives]),
                         ("после рывка", [x for x, a in lives if a]),
                         ("в тихое время", [x for x, a in lives if not a])):
            if xs:
                L.append(f"{name:<22}{len(xs):>7}{statistics.median(xs):>9.0f}мс"
                         f"{pct(xs, 25):>8.0f}мс{pct(xs, 10):>8.0f}мс")
        L += ["", "   Читать так: «медиана 800мс» = половина заявок живёт дольше 0.8 с.",
              "   «10% = 40мс» = каждая десятая исчезает за 40 мс — такие мы не берём никогда."]
    else:
        L.append("   пока нет данных")
    L.append("")

    # 2) выживание цены после сигнала
    L += ["2) ГЛАВНОЕ: ВЫЖИВАЕТ ЛИ ЦЕНА, КОТОРУЮ МЫ ХОТИМ ЗАБРАТЬ",
          "   Момент, когда наш бот решил покупать. Через сколько мс цена ещё на месте?", ""]
    if sv:
        L.append(f"{'задержка заявки':<22}{'та же цена':>12}{'+1¢ допуск':>12}{'из':>7}")
        for t in HORIZONS_MS:
            xs = [r for r in sv if r.get(f"ok_{t}") in ("0", "1")]
            if not xs:
                continue
            a = sum(r[f"ok_{t}"] == "1" for r in xs) / len(xs)
            yt = [r for r in xs if r.get(f"okt_{t}") in ("0", "1")]
            b = f"{sum(r[f'okt_{t}'] == '1' for r in yt) / len(yt):.0%}" if yt else "-"
            L.append(f"{f'{t} мс':<22}{a:>11.0%}{b:>12}{len(xs):>7}")
        L += ["", "   «та же цена» — строго не хуже увиденной; «+1¢ допуск» — как реально",
              "   покупает бот (он готов взять на цент дороже). Смотреть надо на вторую.",
              "   Ниже ~30% на нашей задержке — симулятор врёт в разы, стратегия не работает."]
        L += ["", "   Не достаются ли нам только плохие сделки? Делим выборку на четыре",
              "   равные части по силе рывка: слабые рывки vs сильные. Если у сильных",
              "   доля попаданий заметно ниже — лучшее забирают до нас.", ""]
        def strength(r):
            return abs(f(r.get("move_bps")) or 0.0)
        mv = sorted(sv, key=strength)        # сортируем ТОЛЬКО по силе: при равных
        
        q = len(mv) // 4
        if q >= 15:
            L.append(f"{'сила рывка':<22}{'100мс':>8}{'200мс':>8}{'500мс':>8}{'из':>7}")
            for i, name in enumerate(("слабее всех", "ниже среднего",
                                      "выше среднего", "сильнее всех")):
                grp = mv[i * q:(i + 1) * q] if i < 3 else mv[3 * q:]
                lo, hi = strength(grp[0]), strength(grp[-1])
                cells = ""
                for t in (100, 200, 500):
                    ys = [r for r in grp if r.get(f"okt_{t}") in ("0", "1")]
                    cells += (f"{sum(r[f'okt_{t}'] == '1' for r in ys) / len(ys):>7.0%} "
                              if ys else f"{'-':>8}")
                L.append(f"{f'{name} ({lo:.1f}-{hi:.1f})':<22}{cells}{len(grp):>7}")
            L.append("   Разница в пределах нескольких процентов — отбора худшего не видно.")
        else:
            L.append("   мало сигналов для разбивки (нужно ≥60)")
        cheap = [r for r in sv if (f(r["ask0"]) or 1) <= 0.5]
        if cheap and len(cheap) >= 10:
            k = f"ok_200"
            xs = [r[k] for r in cheap if r.get(k) in ("0", "1")]
            if xs:
                L.append(f"   Отдельно дешёвые билеты (≤50¢), 200мс: "
                         f"{sum(x == '1' for x in xs) / len(xs):.0%} из {len(xs)}")
    else:
        L.append("   пока не было сигналов (нужен рывок на фьючерсах при несвежем аске)")
    L.append("")

    # 3) призраки
    L += ["3) ПРИЗРАКИ: врёт ли запоздавший список заявок, по которому решает симулятор", ""]
    if gh:
        n = len(gh)
        g = sum(r["ghost"] == "1" for r in gh)
        ages = [x for x in (f(r["rest_age_ms"]) for r in gh) if x is not None]
        calls = [x for x in (f(r["call_ms"]) for r in gh) if x is not None]
        diffs = [x for x in (f(r["diff_c"]) for r in gh) if x is not None and x > 0]
        L.append(f"   сверок: {n}, из них призраков (REST дешевле живого стакана): {g} = {g / n:.0%}")
        if diffs:
            L.append(f"   когда призрак — обещает дешевле в среднем на {statistics.mean(diffs):.2f}¢")
        if ages:
            L.append(f"   возраст списка REST: медиана {statistics.median(ages):.0f}мс, "
                     f"90% до {pct(ages, 90):.0f}мс")
        if calls:
            L.append(f"   время ответа API: медиана {statistics.median(calls):.0f}мс, "
                     f"90% до {pct(calls, 90):.0f}мс")
            L.append(f"   -> наша задержка заявки будет НЕ МЕНЬШЕ {statistics.median(calls):.0f}мс;"
                     f" смотри эту строку в таблице выше")
    else:
        L.append("   пока нет сверок")
    # 4) потолок по объёму
    L += ["", "4) ПОТОЛОК ПО ОБЪЁМУ: сколько денег влезает в одну сделку", ""]
    caps = [(f(r.get("usd_at_best")), f(r.get("usd_within_1c")), f(r.get("usd_within_3c")),
             f(r.get("vwap_100usd")), f(r.get("rest_ask"))) for r in gh]
    caps = [c for c in caps if c[0] is not None]
    if caps:
        L.append(f"{'сколько доступно':<26}{'медиана':>10}{'25%':>9}{'10%':>9}")
        for name, i in (("по лучшей цене", 0), ("в пределах +1¢", 1), ("в пределах +3¢", 2)):
            xs = [c[i] for c in caps if c[i] is not None]
            if xs:
                L.append(f"{name:<26}{statistics.median(xs):>9.0f}${pct(xs, 25):>8.0f}$"
                         f"{pct(xs, 10):>8.0f}$")
        slips = [(v - a) * 100 for _, _, _, v, a in caps
                 if v is not None and a is not None and v > 0]
        if slips:
            L.append(f"   Если вложить $100 сразу: средняя цена хуже лучшей на "
                     f"{statistics.median(slips):.2f}¢ (медиана)")
        L += ["", "   Читать так: «в пределах +1¢ медиана 25$» = в половине случаев",
              "   больше $25 в одну сделку не влезет без потери преимущества.",
              "   Это и есть потолок: ставку выше него поднимать бессмысленно."]
    else:
        L.append("   пока нет данных (нужны сверки стакана)")

    L += ["", "КАК ЭТО ЧИТАТЬ ВМЕСТЕ:",
          "Доля попаданий режет ЧИСЛО сделок, а не перевес в каждой. 80% попаданий = 80%",
          "прибыли симулятора, и этого хватает с запасом (для 30%/мес нужно 0.3% от неё).",
          "Опасность не в доле, а в ОТБОРЕ: если не достаются именно лучшие сделки, перевес",
          "в оставшихся падает — на это смотри в разбивке по силе рывка выше.",
          "Чего замер не знает: сколько летит НАША заявка (здесь только чтение, 23мс).",
          "Это даёт только реальная заявка на $1-2."]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Замер реальности исполнения без денег")
    ap.add_argument("cmd", choices=("run", "report"))
    ap.add_argument("--out", default=str(HERE / "data_fill"))
    ap.add_argument("--config", default=str(HERE / "fill_config.json"))
    a = ap.parse_args()
    out = Path(a.out)
    if a.cmd == "report":
        print(report(out))
        return
    cfg = load_config(Path(a.config))
    try:
        asyncio.run(run(cfg, out))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
