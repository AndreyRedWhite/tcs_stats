# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from collections import defaultdict
from datetime import date, datetime, timedelta, time
from decimal import Decimal
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

from tinkoff.invest import AsyncClient, GetOperationsByCursorRequest, OperationState

from tcs_stats.utils import decimal_from_units_nano, round_money, safe_currency, to_utc


# ---- classification helpers --------------------------------------------------


def _is_trade(op_type_name: str) -> bool:
    n = op_type_name.upper()
    return ("BUY" in n) or ("SELL" in n)


def _is_fee(op_type_name: str) -> bool:
    n = op_type_name.upper()
    return ("FEE" in n) or ("COMMISSION" in n) or ("SERVICE" in n)


def _is_tax(op_type_name: str) -> bool:
    return "TAX" in op_type_name.upper()


def _is_dividend(op_type_name: str) -> bool:
    return "DIVIDEND" in op_type_name.upper()


def _is_coupon(op_type_name: str) -> bool:
    return "COUPON" in op_type_name.upper()


def _is_deposit(op_type_name: str) -> bool:
    n = op_type_name.upper()
    return ("INPUT" in n) or ("DEPOSIT" in n)


def _is_withdrawal(op_type_name: str) -> bool:
    n = op_type_name.upper()
    return ("WITHDRAW" in n) or ("OUTPUT" in n)


# ---- models ------------------------------------------------------------------


@dataclass
class InstrumentStats:
    instrument_id: str
    instrument_name: str
    currency: str
    total_trades: int = 0
    buy_trades: int = 0
    sell_trades: int = 0
    positive_trades: int = 0
    negative_trades: int = 0
    cash_in: Decimal = Decimal("0")
    cash_out: Decimal = Decimal("0")
    commissions: Decimal = Decimal("0")
    taxes: Decimal = Decimal("0")
    dividends: Decimal = Decimal("0")
    coupons: Decimal = Decimal("0")
    other_in: Decimal = Decimal("0")
    other_out: Decimal = Decimal("0")

    def net_result(self) -> Decimal:
        return (
            self.cash_in
            - self.cash_out
            - self.commissions
            - self.taxes
            + self.dividends
            + self.coupons
            + self.other_in
            - self.other_out
        )


@dataclass
class StatsResult:
    since: datetime
    until: datetime
    timezone: str
    instruments: List[InstrumentStats]
    totals_by_currency: Dict[str, InstrumentStats]
    daily_totals_by_currency: Dict[date, Dict[str, InstrumentStats]] | None = None


# ---- data helpers ------------------------------------------------------------


async def _iter_operations(
    services,
    account_id: str,
    dt_from_utc: datetime,
    dt_to_utc: datetime,
):
    has_next = True
    cursor: str | None = None
    while has_next:
        req = GetOperationsByCursorRequest(
            account_id=account_id,
            from_=dt_from_utc,
            to=dt_to_utc,
            cursor=cursor or "",
            limit=1000,
            state=OperationState.OPERATION_STATE_EXECUTED,
        )
        resp = await services.operations.get_operations_by_cursor(req)
        for item in resp.items:
            yield item
        has_next = bool(resp.has_next)
        cursor = resp.next_cursor if has_next else None


def _instrument_identity(op) -> Tuple[str, str]:
    uid = getattr(op, "instrument_uid", None)
    figi = getattr(op, "figi", None)
    ticker = getattr(op, "ticker", None)
    name = getattr(op, "name", None)
    position_uid = getattr(op, "position_uid", None)

    instrument_id = uid or figi or position_uid or "UNSPECIFIED"

    display_parts = []
    if ticker:
        display_parts.append(str(ticker))
    if name and name not in display_parts:
        display_parts.append(str(name))
    if not display_parts:
        display_parts.append(str(figi or uid or position_uid or "Без названия"))

    return instrument_id, " - ".join(display_parts)


def _ensure_stats(
    mapping: Dict[Tuple[str, str], InstrumentStats],
    instrument_id: str,
    instrument_name: str,
    currency: str,
) -> InstrumentStats:
    key = (instrument_id, currency)
    if key not in mapping:
        mapping[key] = InstrumentStats(instrument_id, instrument_name, currency)
    return mapping[key]


def _ensure_total(
    mapping: Dict[str, InstrumentStats],
    currency: str,
) -> InstrumentStats:
    if currency not in mapping:
        mapping[currency] = InstrumentStats("__total__", "TOTAL", currency)
    return mapping[currency]


def _apply_amount(stats: InstrumentStats, amount: Decimal, op_type_name: str) -> None:
    if amount == 0:
        return

    if _is_trade(op_type_name):
        stats.total_trades += 1
        if amount > 0:
            stats.sell_trades += 1
            stats.positive_trades += 1
            stats.cash_in += amount
        else:
            stats.buy_trades += 1
            stats.negative_trades += 1
            stats.cash_out += (-amount)
    elif _is_fee(op_type_name):
        stats.commissions += (amount if amount > 0 else -amount)
    elif _is_tax(op_type_name):
        stats.taxes += (amount if amount > 0 else -amount)
    elif _is_dividend(op_type_name):
        stats.dividends += (amount if amount > 0 else -amount)
    elif _is_coupon(op_type_name):
        stats.coupons += (amount if amount > 0 else -amount)
    elif _is_deposit(op_type_name):
        if amount > 0:
            stats.other_in += amount
        else:
            stats.other_out += (-amount)
    elif _is_withdrawal(op_type_name):
        if amount > 0:
            stats.other_in += amount
        else:
            stats.other_out += (-amount)
    else:
        if amount > 0:
            stats.other_in += amount
        else:
            stats.other_out += (-amount)


async def collect_instrument_stats(
    token: str,
    account_id: str,
    since_local: datetime,
    until_local: datetime,
    tz_name: str,
) -> StatsResult:
    tz = ZoneInfo(tz_name)
    since_local = (
        since_local.replace(tzinfo=tz)
        if since_local.tzinfo is None
        else since_local.astimezone(tz)
    )
    until_local = (
        until_local.replace(tzinfo=tz)
        if until_local.tzinfo is None
        else until_local.astimezone(tz)
    )

    stats_by_instrument: Dict[Tuple[str, str], InstrumentStats] = {}
    totals: Dict[str, InstrumentStats] = {}
    daily_totals: defaultdict[date, Dict[str, InstrumentStats]] = defaultdict(dict)

    async with AsyncClient(token) as client:
        since_utc = to_utc(since_local)
        until_utc = to_utc(until_local)

        async for op in _iter_operations(client, account_id, since_utc, until_utc):
            op_type = getattr(op, "operation_type", None) or getattr(op, "type", None)
            op_type_name = getattr(op_type, "name", "UNSPECIFIED")
            payment = getattr(op, "payment", None)
            if payment is None:
                continue

            currency = safe_currency(getattr(payment, "currency", None))
            amount = decimal_from_units_nano(payment.units, payment.nano)
            instrument_id, instrument_name = _instrument_identity(op)

            op_date_raw = getattr(op, "date", None)
            if isinstance(op_date_raw, datetime):
                op_local_date = (
                    op_date_raw.astimezone(tz).date()
                    if op_date_raw.tzinfo
                    else op_date_raw.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz).date()
                )
            else:
                op_local_date = since_local.date()

            inst_stats = _ensure_stats(stats_by_instrument, instrument_id, instrument_name, currency)
            total_stats = _ensure_total(totals, currency)
            daily_total_stats = _ensure_total(daily_totals[op_local_date], currency)

            _apply_amount(inst_stats, amount, op_type_name)
            _apply_amount(total_stats, amount, op_type_name)
            _apply_amount(daily_total_stats, amount, op_type_name)

    day_cursor = since_local.date()
    if until_local > since_local:
        end_of_range = (until_local - timedelta(microseconds=1)).date()
    else:
        end_of_range = since_local.date()
    while day_cursor <= end_of_range:
        _ = daily_totals[day_cursor]
        day_cursor += timedelta(days=1)

    return StatsResult(
        since=since_local,
        until=until_local,
        timezone=tz_name,
        instruments=sorted(
            stats_by_instrument.values(),
            key=lambda s: (s.currency, -s.net_result(), s.instrument_name),
        ),
        totals_by_currency=totals,
        daily_totals_by_currency={day: totals for day, totals in sorted(daily_totals.items())},
    )


# ---- presentation ------------------------------------------------------------


def _format_money(amount: Decimal, currency: str) -> str:
    return f"{round_money(amount, 2):,.2f} {currency}"


def _print_instrument_stats(stats: InstrumentStats) -> None:
    print(f"{stats.instrument_name} [{stats.currency}]")
    print(
        "  Trades: {total} (buys: {buys}, sells: {sells}, positive: {pos}, negative: {neg})".format(
            total=stats.total_trades,
            buys=stats.buy_trades,
            sells=stats.sell_trades,
            pos=stats.positive_trades,
            neg=stats.negative_trades,
        )
    )
    print(
        f"  Cash in: {_format_money(stats.cash_in, stats.currency)}, cash out: {_format_money(stats.cash_out, stats.currency)}"
    )
    if stats.dividends:
        print(f"  Dividends: {_format_money(stats.dividends, stats.currency)}")
    if stats.coupons:
        print(f"  Coupons: {_format_money(stats.coupons, stats.currency)}")
    if stats.commissions:
        print(f"  Commissions: {_format_money(stats.commissions, stats.currency)}")
    if stats.taxes:
        print(f"  Taxes: {_format_money(stats.taxes, stats.currency)}")
    if stats.other_in or stats.other_out:
        print(
            f"  Other in/out: {_format_money(stats.other_in, stats.currency)} / {_format_money(stats.other_out, stats.currency)}"
        )
    print(f"  Net result: {_format_money(stats.net_result(), stats.currency)}")
    print()


def print_report(result: StatsResult) -> None:
    period = "week" if (result.until - result.since) > timedelta(days=1, seconds=1) else "day"
    print(
        f"Period: {result.since.isoformat()} .. {result.until.isoformat()} (timezone: {result.timezone}, {period})"
    )
    print("\n=== Overall totals ===")
    for currency, stats in sorted(result.totals_by_currency.items()):
        _print_instrument_stats(stats)

    if period == "week" and result.daily_totals_by_currency:
        print("=== Totals by day ===")
        for day, totals in sorted(result.daily_totals_by_currency.items()):
            print(day.isoformat())
            if not totals:
                print("  No operations")
                print()
                continue
            for currency, stats in sorted(totals.items()):
                _print_instrument_stats(stats)

    print("=== Per instrument ===")
    for stats in result.instruments:
        if stats.instrument_id == "__total__":
            continue
        _print_instrument_stats(stats)


# ---- CLI ---------------------------------------------------------------------


def _start_of_today(now: datetime) -> datetime:
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def _start_of_week(now: datetime) -> datetime:
    monday = now - timedelta(days=now.isoweekday() - 1)
    return datetime.combine(monday.date(), time.min, tzinfo=now.tzinfo)


def _determine_period(tz_name: str, week: bool) -> Tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    if week:
        start = _start_of_week(now)
        planned_end = start + timedelta(days=7)
    else:
        start = _start_of_today(now)
        planned_end = start + timedelta(days=1)

    until = min(planned_end, now)
    if until <= start:
        until = start + timedelta(seconds=1)
    return start, until


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tcs_stats.report",
        description="Show aggregated trade statistics for the current day or week.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("TINKOFF_TOKEN"),
        help="Tinkoff Invest API token (or env TINKOFF_TOKEN)",
    )
    parser.add_argument(
        "--account-id",
        default=os.getenv("ACCOUNT_ID"),
        help="Account id (or env ACCOUNT_ID)",
    )
    parser.add_argument(
        "--tz",
        default=os.getenv("TIMEZONE", "Europe/Moscow"),
        help="IANA timezone name. Default: Europe/Moscow",
    )
    parser.add_argument(
        "--week",
        action="store_true",
        help="Aggregate statistics for the current week (default: current day)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    if not args.token:
        raise SystemExit("TINKOFF_TOKEN is required (env or --token)")
    if not args.account_id:
        raise SystemExit("ACCOUNT_ID is required (env or --account-id)")

    since, until = _determine_period(args.tz, args.week)

    result = await collect_instrument_stats(
        token=args.token,
        account_id=args.account_id,
        since_local=since,
        until_local=until,
        tz_name=args.tz,
    )

    print_report(result)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
