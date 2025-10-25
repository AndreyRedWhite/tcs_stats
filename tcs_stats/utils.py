# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional


def decimal_from_units_nano(units: int, nano: int) -> Decimal:
    """Convert Tinkoff 'units' + 'nano' to Decimal.

    Args:
        units: Integer monetary units.
        nano: Nanounits (may be negative for negative amounts).

    Returns:
        Decimal monetary value with 9 digits after decimal point.
    """
    sign = -1 if (units < 0 or nano < 0) else 1
    abs_units = abs(units)
    abs_nano = abs(nano)
    val = Decimal(abs_units) + (Decimal(abs_nano) / Decimal("1000000000"))
    return (val if sign > 0 else -val).quantize(Decimal("0.000000001"))


def to_utc(dt: datetime) -> datetime:
    """Ensure timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def round_money(value: Decimal, places: int = 4) -> float:
    """Round Decimal to float with given places (for JSON/Excel)."""
    return float(value.quantize(Decimal(10) ** -places, rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class MoneyLite:
    """Lightweight money holder for aggregation."""
    amount: Decimal
    currency: str

    def __add__(self, other: "MoneyLite") -> "MoneyLite":
        if self.currency != other.currency:
            raise ValueError("Currency mismatch in MoneyLite addition")
        return MoneyLite(self.amount + other.amount, self.currency)

    def as_float(self, places: int = 4) -> float:
        return round_money(self.amount, places)


def safe_currency(cur: Optional[str]) -> str:
    return cur or "RUB"
