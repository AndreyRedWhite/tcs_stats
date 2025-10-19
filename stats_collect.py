# stats_collect.py
from __future__ import annotations
import asyncio
import json
import os
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
import pytz

from tinkoff.invest import AsyncClient, TradeDirection
from tinkoff.invest.utils import quotation_to_decimal

from instruments_cache import InstrumentCache, SecSpec

MSK = pytz.timezone("Europe/Moscow")

@dataclass
class RoundTrip:
    open_time: str
    close_time: str
    qty: int
    buy_price: float
    sell_price: float
    pnl_gross: float  # до комиссий, в рублях
    is_win: bool

@dataclass
class InstrumentStats:
    figi: str
    uid: str
    ticker: str
    class_code: str
    n_trades: int
    n_roundtrips: int
    wins: int
    losses: int
    pnl_gross: float
    commissions: float
    pnl_net: float
    roundtrips: List[RoundTrip]

def dt_range(now: datetime, kind: str) -> Tuple[datetime, datetime]:
    now = now.astimezone(MSK)
    if kind == "day":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif kind == "week":
        # ISO: неделя с понедельника
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    elif kind == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif kind == "year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError("Unknown kind")
    return start, now

def money_to_float(money) -> float:
    return float(money.units) + money.nano/1e9

async def fetch_trades(client: AsyncClient, account_id: str, dt_from: datetime, dt_to: datetime):
    # gRPC get_trades: сделки по всем инструментам за период
    r = await client.orders.get_trades(account_id=account_id, from_=dt_from, to=dt_to)
    return r.trades  # список Trade

async def fetch_commissions(client: AsyncClient, account_id: str, dt_from: datetime, dt_to: datetime):
    # “Операции по счёту” курсором — надёжнее для комиссий
    # В ответе много типов, отфильтруем комиссионные/биржсборы
    cursor = None
    total = []
    while True:
        resp = await client.operations.get_operations_by_cursor(
            account_id=account_id, from_=dt_from, to=dt_to, cursor=cursor, limit=1000
        )
        total.extend(resp.items)
        if not resp.has_next:
            break
        cursor = resp.next_cursor
    # вернём как есть; дальше фильтруем по instrument_uid и типам
    return total

def fifo_roundtrips_for_instrument(trades, spec: SecSpec) -> Tuple[List[RoundTrip], int]:
    """
    Формируем раунды по FIFO:
    - BUY добавляет в очередь
    - SELL закрывает из головы очереди
    Для фьючерсов переводим пункты в рубли через fut.point_to_rub.
    Возвращаем (список закрытых раундов, всего сделок по инструменту)
    """
    inv = deque()  # [(price, qty, time)]
    trips: List[RoundTrip] = []
    n_trades = 0
    point2rub = spec.fut.point_to_rub if (spec.is_future and spec.fut) else None
    lot = spec.lot or 1

    # Отсортируем по времени
    trades_sorted = sorted(trades, key=lambda t: t.date_time)

    for t in trades_sorted:
        n_trades += 1
        px = float(quotation_to_decimal(t.price))
        qty = t.quantity * lot
        ts = t.date_time.astimezone(MSK).isoformat()

        if t.direction == TradeDirection.TRADE_DIRECTION_BUY:
            inv.append([px, qty, ts])
        else:  # SELL
            q = qty
            while q > 0 and inv:
                bpx, bqty, bts = inv[0]
                take = min(bqty, q)
                # PnL расчёт
                if spec.is_future and point2rub:
                    pnl = (px - bpx) * point2rub * take
                else:
                    pnl = (px - bpx) * take  # валюта инструмента; акции/ETF рубли — ок

                trips.append(RoundTrip(
                    open_time=bts, close_time=ts, qty=take,
                    buy_price=bpx, sell_price=px,
                    pnl_gross=pnl, is_win=pnl >= 0
                ))
                bqty -= take
                q -= take
                if bqty == 0:
                    inv.popleft()
                else:
                    inv[0][1] = bqty

    return trips, n_trades

def sum_commissions_for_instrument(ops_items, spec: SecSpec) -> float:
    """
    Склеиваем комиссии по instrument_uid/figi.
    В OperationsByCursor у позиций есть поля instrument_uid/figi и тип операции.
    Берём все, где есть commission/payment < 0 и operation_type в “комиссионных”.
    """
    COMM_KEYS = {"OPERATION_TYPE_BROKER_FEE", "OPERATION_TYPE_EXCHANGE_FEE",
                 "OPERATION_TYPE_SERVICE_FEE", "OPERATION_TYPE_MARGIN_FEE",
                 "OPERATION_TYPE_FUTURES_FEE", "OPERATION_TYPE_SUCCESS_FEE"}
    total = 0.0
    for it in ops_items:
        # фильтр по конкретному инструменту (если доступен UID/FIGI)
        same = False
        if getattr(it, "instrument_uid", "") and it.instrument_uid == spec.uid:
            same = True
        elif getattr(it, "figi", "") and it.figi == spec.figi:
            same = True
        if not same:
            continue
        # фильтр типа
        if it.operation_type.name not in COMM_KEYS:
            continue
        if it.payment:
            total += money_to_float(it.payment)  # комиссии обычно отрицательные
        elif it.commission:
            total += money_to_float(it.commission)
    # Сделаем знак понятным: комиссия как положительная величина расхода
    return abs(total)

async def calc_window(client: AsyncClient, account_id: str, cache: InstrumentCache, kind: str):
    start, end = dt_range(datetime.now(tz=MSK), kind)
    trades = await fetch_trades(client, account_id, start, end)
    ops = await fetch_commissions(client, account_id, start, end)

    # группируем трейды по инструменту
    by_key: Dict[str, List] = defaultdict(list)
    for t in trades:
        key = t.figi or t.instrument_uid
        by_key[key].append(t)

    result: Dict[str, InstrumentStats] = {}

    for key, lst in by_key.items():
        # резолвим спецификацию
        any_t = lst[0]
        spec = await cache.resolve(client, figi=any_t.figi or None, uid=any_t.instrument_uid or None)

        trips, n_trades = fifo_roundtrips_for_instrument(lst, spec)
        pnl_gross = sum(x.pnl_gross for x in trips)
        wins = sum(1 for x in trips if x.is_win)
        losses = sum(1 for x in trips if not x.is_win)

        commissions = sum_commissions_for_instrument(ops, spec)
        pnl_net = pnl_gross - commissions

        result[spec.ticker] = InstrumentStats(
            figi=spec.figi, uid=spec.uid, ticker=spec.ticker, class_code=spec.class_code,
            n_trades=n_trades, n_roundtrips=len(trips), wins=wins, losses=losses,
            pnl_gross=round(pnl_gross, 2), commissions=round(commissions, 2), pnl_net=round(pnl_net, 2),
            roundtrips=[x for x in trips]
        )

    # Важно: инструменты без сделок, но с комиссиями (редко) здесь не появятся — это ок.
    return {
        "window": kind,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "stats": {k: asdict(v) for k, v in result.items()}
    }

async def main():
    load_dotenv()
    token = os.getenv("TINKOFF_TOKEN")
    account_id = os.getenv("TINKOFF_ACCOUNT_ID")
    if not token or not account_id:
        raise SystemExit("Set TINKOFF_TOKEN and TINKOFF_ACCOUNT_ID in .env")

    cache = InstrumentCache()
    async with AsyncClient(token) as client:
        windows = {}
        for kind in ("day", "week", "month", "year"):
            windows[kind] = await calc_window(client, account_id, cache, kind)

    out = {
        "generated_at": datetime.now(tz=MSK).isoformat(),
        "account_id": account_id,
        "windows": windows
    }
    with open("tcs_stats.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("Saved -> tcs_stats.json")


if __name__ == "__main__":
    asyncio.run(main())
