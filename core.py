"""Чистая логика симуляции: без сети, без asyncio.

Здесь всё, что нужно проверить тестами: разбор сделок, проход по стакану,
комиссии, портфель, расчёт по итогам рынка.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from typing import Optional

# ---------------------------------------------------------------- сделки лидера

@dataclass
class LeaderTrade:
    wallet: str
    name: str
    ts: float            # время сделки лидера, unix-секунды
    asset: str           # token id исхода
    condition_id: str
    side: str            # BUY / SELL
    price: float
    size: float          # акции
    usdc: float          # сколько он реально заплатил/получил
    slug: str
    title: str
    outcome: str
    tx: str

    @property
    def key(self) -> str:
        return f"{self.tx}:{self.asset}:{self.side}:{self.size}:{self.price}"

    @property
    def implied_fee_coef(self) -> Optional[float]:
        """Коэффициент комиссии, который заплатил лидер: (usdc - size*price) / (size*p*(1-p)).

        Наблюдение по реальным сделкам: комиссия тейкера на крипто-рынках
        = 0.07 * акции * p * (1 - p). Для мейкерских исполнений будет 0.
        """
        if self.side != "BUY" or not (0 < self.price < 1) or self.size <= 0:
            return None
        base = self.size * self.price * (1 - self.price)
        return (self.usdc - self.size * self.price) / base


def parse_trade(d: dict) -> Optional[LeaderTrade]:
    """Принимает объект из Data API /activity или payload из RTDS activity/trades."""
    try:
        if d.get("type") not in (None, "TRADE"):
            return None
        side = str(d.get("side", "")).upper()
        if side not in ("BUY", "SELL"):
            return None
        price = float(d["price"])
        size = float(d["size"])
        usdc = float(d.get("usdcSize") or price * size)
        ts = float(d.get("timestamp") or 0)
        if ts > 1e12:          # на всякий случай: миллисекунды -> секунды
            ts /= 1000.0
        return LeaderTrade(
            wallet=str(d.get("proxyWallet", "")).lower(),
            name=str(d.get("name") or d.get("pseudonym") or ""),
            ts=ts,
            asset=str(d["asset"]),
            condition_id=str(d.get("conditionId", "")),
            side=side,
            price=price,
            size=size,
            usdc=usdc,
            slug=str(d.get("slug") or d.get("eventSlug") or ""),
            title=str(d.get("title", "")),
            outcome=str(d.get("outcome", "")),
            tx=str(d.get("transactionHash", "")),
        )
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------- время закрытия

_SLUG_RE = re.compile(r"-(5m|15m|1h|4h)-(\d{9,11})$")
_DUR = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}


def end_ts_from_slug(slug: str) -> Optional[float]:
    """btc-updown-5m-1793091900 -> 1793091900 + 300. Для других слагов None."""
    m = _SLUG_RE.search(slug or "")
    if not m:
        return None
    return float(m.group(2)) + _DUR[m.group(1)]


# ---------------------------------------------------------------- стакан и комиссии

def fee(shares: float, price: float, coef: float) -> float:
    return coef * shares * price * (1.0 - price)


def _levels(raw) -> list[tuple[float, float]]:
    out = []
    for lv in raw or []:
        try:
            p, s = float(lv["price"]), float(lv["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if s > 0:
            out.append((p, s))
    return out


@dataclass
class Fill:
    shares: float = 0.0
    notional: float = 0.0     # сумма price*shares без комиссии
    fee: float = 0.0
    levels: int = 0
    best: Optional[float] = None   # лучшая цена в стакане в момент входа

    @property
    def vwap(self) -> Optional[float]:
        return self.notional / self.shares if self.shares else None


def walk_book(book: dict, side: str, shares: float, limit: float, fee_coef: float) -> Fill:
    """Имитирует ордер FAK: забирает уровни стакана до лимитной цены.

    BUY идёт по asks от дешёвых к дорогим, SELL — по bids от дорогих к дешёвым.
    Сортируем сами: CLOB отдаёт уровни не в «удобном» порядке.
    """
    if side == "BUY":
        lv = sorted(_levels(book.get("asks")), key=lambda x: x[0])
        ok = lambda p: p <= limit + 1e-9
    else:
        lv = sorted(_levels(book.get("bids")), key=lambda x: -x[0])
        ok = lambda p: p >= limit - 1e-9
    f = Fill(best=lv[0][0] if lv else None)
    left = shares
    for p, s in lv:
        if left <= 1e-9 or not ok(p):
            break
        take = min(s, left)
        f.shares += take
        f.notional += take * p
        f.fee += fee(take, p, fee_coef)
        f.levels += 1
        left -= take
    return f


# ---------------------------------------------------------------- портфель

@dataclass
class Position:
    asset: str
    shares: float
    cost: float          # всего потрачено, включая комиссии
    slug: str
    condition_id: str
    outcome: str
    title: str
    leader: str
    end_ts: Optional[float] = None


@dataclass
class Portfolio:
    cash: float
    start_cash: float
    positions: dict = field(default_factory=dict)   # asset -> Position
    realized: float = 0.0
    fees: float = 0.0
    wins: int = 0
    losses: int = 0

    def buy(self, t: LeaderTrade, shares: float, notional: float, fee_paid: float,
            end_ts: Optional[float]) -> None:
        total = notional + fee_paid
        self.cash -= total
        self.fees += fee_paid
        pos = self.positions.get(t.asset)
        if pos is None:
            self.positions[t.asset] = Position(t.asset, shares, total, t.slug, t.condition_id,
                                               t.outcome, t.title, t.name or t.wallet, end_ts)
        else:
            pos.shares += shares
            pos.cost += total

    def sell(self, asset: str, shares: float, notional: float, fee_paid: float) -> float:
        """Продаёт часть позиции, возвращает реализованный PnL этой продажи."""
        pos = self.positions.get(asset)
        if not pos or shares <= 0:
            return 0.0
        shares = min(shares, pos.shares)
        cost_part = pos.cost * shares / pos.shares
        proceeds = notional - fee_paid
        pnl = proceeds - cost_part
        self.cash += proceeds
        self.fees += fee_paid
        self.realized += pnl
        pos.shares -= shares
        pos.cost -= cost_part
        if pos.shares <= 1e-9:
            del self.positions[asset]
        return pnl

    def settle(self, asset: str, payout_per_share: float) -> Optional[float]:
        pos = self.positions.pop(asset, None)
        if not pos:
            return None
        payout = pos.shares * payout_per_share
        pnl = payout - pos.cost
        self.cash += payout
        self.realized += pnl
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        return pnl

    def open_cost(self) -> float:
        return sum(p.cost for p in self.positions.values())

    def equity_at_cost(self) -> float:
        return self.cash + self.open_cost()

    def to_dict(self) -> dict:
        d = asdict(self)
        d["positions"] = {k: asdict(v) for k, v in self.positions.items()}
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Portfolio":
        pos = {k: Position(**v) for k, v in d.get("positions", {}).items()}
        p = cls(**{k: v for k, v in d.items() if k != "positions"})
        p.positions = pos
        return p


# ---------------------------------------------------------------- итоги рынка

def _as_list(v):
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return []
    return list(v or [])


def resolution_payouts(market: dict) -> Optional[dict]:
    """Из ответа gamma-api /markets -> {token_id: выплата на акцию}, если рынок закрыт.

    Ждём, пока цены исходов станут ровно 0/1 — это и есть итог.
    """
    if not market or not market.get("closed"):
        return None
    tokens = [str(t) for t in _as_list(market.get("clobTokenIds"))]
    prices = []
    for p in _as_list(market.get("outcomePrices")):
        try:
            prices.append(float(p))
        except (TypeError, ValueError):
            return None
    if not tokens or len(tokens) != len(prices):
        return None
    if not all(abs(p - round(p)) < 1e-6 for p in prices):
        return None          # ещё не финальные цены
    return {t: round(p) for t, p in zip(tokens, prices)}


def market_end_ts(market: dict) -> Optional[float]:
    from datetime import datetime
    v = (market or {}).get("endDate") or (market or {}).get("end_date_iso")
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None
