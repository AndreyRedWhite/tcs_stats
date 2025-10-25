# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List
from zoneinfo import ZoneInfo

from tinkoff.invest import (
    AsyncClient,
    OperationState,
    GetOperationsByCursorRequest,
)
from tinkoff.invest.async_services import AsyncServices

from tcs_stats.models import (
    MetaInfo,
    RunningTotals,
    StatsJSON,
    WindowRecord,
    WindowKind,
)
from tcs_stats.time_windows import split_into_windows, Window
from tcs_stats.utils import decimal_from_units_nano, safe_currency, to_utc, round_money


# ---- classification helpers -------------------------------------------------


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


# ---- core aggregation --------------------------------------------------------


async def _iter_operations(
    services,
    account_id: str,
    dt_from_utc: datetime,
    dt_to_utc: datetime,
):
    """Yield operations via cursor pagination (executed only)."""
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
            # without_trades=False  # по умолчанию False, сделки вернутся; оставляем как есть
        )
        resp = await services.operations.get_operations_by_cursor(req)
        for item in resp.items:
            yield item
        has_next = bool(resp.has_next)
        cursor = resp.next_cursor if has_next else None



def _init_bucket() -> Dict[str, RunningTotals]:
    return defaultdict(RunningTotals)  # per-currency totals


def _bucket_key(kind: WindowKind, start: datetime, currency: str) -> str:
    return f"{kind}::{start.isoformat()}::{currency}"


async def collect_stats(
    token: str,
    account_id: str,
    since_local: datetime,
    until_local: datetime,
    tz_name: str = "Europe/Moscow",
    kinds: Iterable[WindowKind] = ("day", "week", "month", "year"),
) -> StatsJSON:
    """Collect statistics into structured JSON.

    Args:
        token: Tinkoff Invest API token.
        account_id: Target account id.
        since_local: Inclusive lower bound in local tz.
        until_local: Exclusive upper bound in local tz.
        tz_name: IANA timezone name, default Europe/Moscow.
        kinds: Window kinds to compute.

    Returns:
        StatsJSON dict ready to dump.
    """
    tz = ZoneInfo(tz_name)
    since_local = since_local.astimezone(tz)
    until_local = until_local.astimezone(tz)

    windows = split_into_windows(since_local, until_local, tz, kinds)
    # Index windows by (kind, start) to place ops fast
    by_kind_start: Dict[tuple[str, datetime], Window] = {
        (w.kind, w.start): w for w in windows
    }

    # Pre-build ordered lists per kind for quick lookup
    kind_to_windows: Dict[str, List[Window]] = defaultdict(list)
    for w in windows:
        kind_to_windows[w.kind].append(w)

    # Aggregation buckets: key -> currency -> totals
    buckets: Dict[str, Dict[str, RunningTotals]] = defaultdict(_init_bucket)

    async with AsyncClient(token) as client:
        services = client
        since_utc = since_local.astimezone(timezone.utc)
        until_utc = until_local.astimezone(timezone.utc)

        async for op in _iter_operations(services, account_id, since_utc, until_utc):
            # Defensive parsing (schema may evolve)
            # op_type_name = getattr(op.operation_type, "name", "UNSPECIFIED")
            op_type = getattr(op, "operation_type", None) or getattr(op, "type", None)
            op_type_name = getattr(op_type, "name", "UNSPECIFIED")
            cur = safe_currency(getattr(getattr(op, "payment", None), "currency", None))
            pay = getattr(op, "payment", None)
            if pay is None:
                # Some operations might not have direct payment (ignore for cashflow)
                continue

            amount = decimal_from_units_nano(pay.units, pay.nano)

            # Convert op timestamp to local tz to map into windows
            op_dt = getattr(op, "date", None)
            if op_dt is None:
                continue
            op_local = op_dt.astimezone(tz)

            # Place operation into all windows that cover its timestamp.
            # For each requested kind we have disjoint, ordered windows.
            for kind, win_list in kind_to_windows.items():
                # Binary search would be faster; linear is OK for daily usage size.
                for w in win_list:
                    if w.start <= op_local < w.end:
                        totals = buckets[_bucket_key(w.kind, w.start, cur)][cur]
                        # Update by category
                        if _is_trade(op_type_name):
                            # Buy < 0 cash, Sell > 0 cash; turnover counts abs cash.
                            if amount < 0:
                                totals.trade_buy_cash += (-amount)
                            else:
                                totals.trade_sell_cash += amount
                            totals.turnover += abs(amount)
                        elif _is_fee(op_type_name):
                            totals.commissions += (-amount if amount < 0 else amount)
                        elif _is_tax(op_type_name):
                            totals.taxes += (-amount if amount < 0 else amount)
                        elif _is_dividend(op_type_name):
                            totals.dividends += (amount if amount > 0 else -amount)
                        elif _is_coupon(op_type_name):
                            totals.coupons += (amount if amount > 0 else -amount)
                        elif _is_deposit(op_type_name):
                            totals.deposits += (amount if amount > 0 else -amount)
                        elif _is_withdrawal(op_type_name):
                            totals.withdrawals += (-amount if amount < 0 else amount)
                        else:
                            totals.other += amount
                        break  # found the window; move to next kind/op

    # Build output list
    out_windows: List[WindowRecord] = []
    for key, by_cur in buckets.items():
        kind_str, start_iso, _cur_key = key.split("::")
        for currency, t in by_cur.items():
            net_excl = (t.trade_sell_cash + t.dividends + t.coupons) - (
                t.trade_buy_cash + t.commissions + t.taxes
            )
            net_incl = net_excl + t.deposits - t.withdrawals
            out_windows.append(
                {
                    "kind": kind_str,  # type: ignore
                    "start": start_iso,
                    "end": next(
                        w.end.isoformat()
                        for w in windows
                        if w.kind == kind_str and w.start.isoformat() == start_iso
                    ),
                    "currency": currency,
                    "stats": {
                        "turnover": round_money(t.turnover),
                        "trade_buy_cash": round_money(t.trade_buy_cash),
                        "trade_sell_cash": round_money(t.trade_sell_cash),
                        "commissions": round_money(t.commissions),
                        "taxes": round_money(t.taxes),
                        "dividends": round_money(t.dividends),
                        "coupons": round_money(t.coupons),
                        "deposits": round_money(t.deposits),
                        "withdrawals": round_money(t.withdrawals),
                        "other": round_money(t.other),
                        "net_cashflow_excl_deposits": round_money(net_excl),
                        "net_cashflow_incl_deposits": round_money(net_incl),
                    },
                }
            )

    meta: MetaInfo = {
        "timezone": tz_name,
        "account_id": account_id,
        "generated_at": datetime.now(tz=tz).isoformat(),
        "since": since_local.isoformat(),
        "until": until_local.isoformat(),
        "sdk": "tinkoff.invest (async, operations.get_operations_by_cursor)",
    }

    result: StatsJSON = {"meta": meta, "windows": sorted(out_windows, key=lambda x: (x["kind"], x["start"], x["currency"]))}
    return result


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="tcs_stats.collect",
        description="Collect async Tinkoff Invest account stats into JSON by time windows.",
    )
    p.add_argument("--token", default=os.getenv("TINKOFF_TOKEN"), help="Tinkoff Invest API token (or env TINKOFF_TOKEN)")
    p.add_argument("--account-id", default=os.getenv("ACCOUNT_ID"), required=False, help="Account id (or env ACCOUNT_ID)")
    p.add_argument("--since", type=str, default=None, help="Lower bound in YYYY-MM-DD (local Europe/Moscow). Default: 365 days ago.")
    p.add_argument("--until", type=str, default=None, help="Exclusive upper bound in YYYY-MM-DD. Default: tomorrow.")
    p.add_argument("--tz", type=str, default=os.getenv("TIMEZONE", "Europe/Moscow"), help="IANA TZ, default Europe/Moscow")
    p.add_argument(
        "--windows",
        type=str,
        default="day,week,month,year",
        help="Comma-separated windows: day,week,month,year",
    )
    p.add_argument("--out", type=Path, default=Path("out") / "stats.json", help="Output JSON path")
    return p.parse_args()


async def _amain() -> None:
    args = _parse_args()
    if not args.token:
        raise SystemExit("TINKOFF_TOKEN is required (env or --token).")
    if not args.account_id:
        raise SystemExit("ACCOUNT_ID is required (env or --account-id).")

    tz = ZoneInfo(args.tz)
    today_local = datetime.now(tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    since = (
        datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=tz)
        if args.since
        else (today_local - timedelta(days=365))
    )
    until = (
        datetime.strptime(args.until, "%Y-%m-%d").replace(tzinfo=tz)
        if args.until
        else (today_local + timedelta(days=1))
    )
    kinds: List[WindowKind] = [k.strip() for k in args.windows.split(",") if k.strip()]

    data = await collect_stats(
        token=args.token,
        account_id=args.account_id,
        since_local=since,
        until_local=until,
        tz_name=args.tz,
        kinds=kinds,  # type: ignore
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Wrote JSON: {args.out}")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
