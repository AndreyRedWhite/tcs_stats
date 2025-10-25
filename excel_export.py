# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd


def _to_naive_datetime(series: pd.Series) -> pd.Series:
    """Parse ISO datetimes and strip timezone to make them Excel-friendly.

    Excel не поддерживает TZ-aware datetime. Мы оставляем локальное время
    (как в исходных строках) и удаляем информацию о TZ.
    """
    s = pd.to_datetime(series, errors="coerce")
    try:
        # Если серия tz-aware — удалить таймзону (без конвертации)
        return s.dt.tz_localize(None)
    except TypeError:
        # Уже tz-naive — вернуть как есть
        return s


def _flatten(json_path: Path) -> pd.DataFrame:
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    rows: List[Dict] = []
    for w in data.get("windows", []):
        stats = w.get("stats", {}) or {}
        rows.append(
            dict(
                kind=w.get("kind"),
                start=w.get("start"),
                end=w.get("end"),
                currency=w.get("currency"),
                turnover=stats.get("turnover", 0.0),
                trade_buy_cash=stats.get("trade_buy_cash", 0.0),
                trade_sell_cash=stats.get("trade_sell_cash", 0.0),
                commissions=stats.get("commissions", 0.0),
                taxes=stats.get("taxes", 0.0),
                dividends=stats.get("dividends", 0.0),
                coupons=stats.get("coupons", 0.0),
                deposits=stats.get("deposits", 0.0),
                withdrawals=stats.get("withdrawals", 0.0),
                other=stats.get("other", 0.0),
                net_excl=stats.get("net_cashflow_excl_deposits", 0.0),
                net_incl=stats.get("net_cashflow_incl_deposits", 0.0),
            )
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Парсим и убираем таймзону -> tz-naive (Excel совместимо)
    df["start"] = _to_naive_datetime(df["start"])
    df["end"] = _to_naive_datetime(df["end"])

    return df.sort_values(["kind", "start", "currency"]).reset_index(drop=True)


def export_excel(json_path: Path, xlsx_path: Path) -> None:
    df = _flatten(json_path)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(
        xlsx_path,
        engine="xlsxwriter",
        datetime_format="yyyy-mm-dd hh:mm",
        date_format="yyyy-mm-dd",
    ) as writer:
        # Даже если df пустой — создадим файл с пустым листом Data.
        df.to_excel(writer, index=False, sheet_name="Data")

        # Развороты по видам окна
        for kind in ["day", "week", "month", "year"]:
            dfk = df[df["kind"] == kind]
            if dfk.empty:
                continue
            dfk_out = (
                dfk[
                    [
                        "start",
                        "currency",
                        "turnover",
                        "net_excl",
                        "net_incl",
                        "commissions",
                        "taxes",
                        "dividends",
                        "coupons",
                    ]
                ]
                .sort_values(["currency", "start"])
            )
            dfk_out.to_excel(writer, index=False, sheet_name=kind.capitalize())

        # Summary + графики: приоритет — month, иначе week, иначе skip
        chart_kind = None
        for cand in ("month", "week"):
            if (not df.empty) and (df["kind"] == cand).any():
                chart_kind = cand
                break

        if chart_kind:
            sheet_name = "Summary"
            dfc = df[df["kind"] == chart_kind].copy().sort_values(["start", "currency"])
            dfc.to_excel(writer, index=False, sheet_name=sheet_name)

            if not dfc.empty:
                wb = writer.book
                ws = writer.sheets[sheet_name]

                # Первая валюта для графиков
                first_cur = dfc["currency"].iloc[0]
                dff = dfc[dfc["currency"] == first_cur].copy()
                if not dff.empty:
                    start_row = 1  # первая строка с данными (после заголовка)
                    col_idx_date = dff.columns.get_loc("start")
                    col_idx_net = dff.columns.get_loc("net_excl")

                    # Столбчатая диаграмма net_excl
                    chart_bar = wb.add_chart({"type": "column"})
                    chart_bar.add_series(
                        {
                            "name": f"{chart_kind} net_excl ({first_cur})",
                            "categories": [
                                sheet_name,
                                start_row,
                                col_idx_date,
                                start_row + len(dff) - 1,
                                col_idx_date,
                            ],
                            "values": [
                                sheet_name,
                                start_row,
                                col_idx_net,
                                start_row + len(dff) - 1,
                                col_idx_net,
                            ],
                        }
                    )
                    chart_bar.set_title(
                        {"name": f"{chart_kind.capitalize()} net cashflow excl deposits ({first_cur})"}
                    )
                    chart_bar.set_x_axis({"name": "Start"})
                    chart_bar.set_y_axis({"name": first_cur})
                    ws.insert_chart("L2", chart_bar, {"x_scale": 1.4, "y_scale": 1.3})

                    # Кумулятивная линия по net_excl
                    dff["cum_net_excl"] = dff["net_excl"].cumsum()
                    # Вписываем вспомогательный столбец на лист Summary (рядом с таблицей)
                    base_row = start_row
                    cum_col = len(dfc.columns)  # следующая свободная колонка
                    ws.write(0, cum_col, "cum_net_excl")
                    for i, v in enumerate(dff["cum_net_excl"].tolist()):
                        ws.write(base_row + i, cum_col, float(v))

                    chart_line = wb.add_chart({"type": "line"})
                    chart_line.add_series(
                        {
                            "name": f"Cumulative net_excl ({first_cur})",
                            "categories": [
                                sheet_name,
                                start_row,
                                col_idx_date,
                                start_row + len(dff) - 1,
                                col_idx_date,
                            ],
                            "values": [
                                sheet_name,
                                start_row,
                                cum_col,
                                start_row + len(dff) - 1,
                                cum_col,
                            ],
                        }
                    )
                    chart_line.set_title(
                        {"name": f"Cumulative {chart_kind} net_excl ({first_cur})"}
                    )
                    chart_line.set_x_axis({"name": "Start"})
                    chart_line.set_y_axis({"name": first_cur})
                    ws.insert_chart("L20", chart_line, {"x_scale": 1.4, "y_scale": 1.3})


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="tcs_stats.excel_export",
        description="Convert stats JSON into Excel with basic charts.",
    )
    p.add_argument("--in", dest="inp", type=Path, required=True, help="Input JSON path")
    p.add_argument("--out", dest="out", type=Path, default=Path("out") / "stats.xlsx", help="Output XLSX path")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    export_excel(args.inp, args.out)
    print(f"Wrote Excel: {args.out}")


if __name__ == "__main__":
    main()
