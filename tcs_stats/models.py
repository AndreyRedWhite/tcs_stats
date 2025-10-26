# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Literal, Optional, TypedDict, NotRequired

WindowKind = Literal["day", "week", "month", "year"]


class CurrencyBreakdown(TypedDict, total=False):
    """Flat numeric fields for a single currency inside a window."""
    turnover: float
    trade_buy_cash: float
    trade_sell_cash: float
    commissions: float
    taxes: float
    dividends: float
    coupons: float
    deposits: float
    withdrawals: float
    other: float
    net_cashflow_excl_deposits: float
    net_cashflow_incl_deposits: float


class WindowRecord(TypedDict):
    kind: WindowKind
    start: str
    end: str
    currency: str
    stats: CurrencyBreakdown
    instruments: NotRequired[List["InstrumentBreakdown"]]


class InstrumentBreakdown(TypedDict):
    instrument_id: str
    instrument_name: str
    currency: str
    stats: CurrencyBreakdown


class MetaInfo(TypedDict):
    timezone: str
    account_id: str
    generated_at: str
    since: str
    until: str
    sdk: str


class StatsJSON(TypedDict):
    meta: MetaInfo
    windows: List[WindowRecord]


@dataclass
class RunningTotals:
    turnover: Decimal = Decimal("0")
    trade_buy_cash: Decimal = Decimal("0")
    trade_sell_cash: Decimal = Decimal("0")
    commissions: Decimal = Decimal("0")
    taxes: Decimal = Decimal("0")
    dividends: Decimal = Decimal("0")
    coupons: Decimal = Decimal("0")
    deposits: Decimal = Decimal("0")
    withdrawals: Decimal = Decimal("0")
    other: Decimal = Decimal("0")
