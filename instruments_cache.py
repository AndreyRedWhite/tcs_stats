from __future__ import annotations
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Dict

from tinkoff.invest import AsyncClient, InstrumentIdType


@dataclass
class FutSpec:
    figi: str
    uid: str
    ticker: str
    class_code: str
    min_price_increment: float
    min_price_increment_amount: float  # руб. за минимальный шаг
    lot: int

    @property
    def point_to_rub(self) -> float:
        # Сколько рублей в 1 пункте фьючерса:
        # напр., если min_price_increment=1, min_price_increment_amount=10 → 10 ₽/пункт
        return self.min_price_increment_amount / self.min_price_increment if self.min_price_increment else 0.0


@dataclass
class SecSpec:
    figi: str
    uid: str
    ticker: str
    class_code: str
    lot: int
    currency: str
    is_future: bool
    fut: Optional[FutSpec] = None


class InstrumentCache:
    """Ленивый кэш инструментов по FIGI/UID — чтобы не дергать справочник сотни раз."""
    def __init__(self):
        self.by_figi: Dict[str, SecSpec] = {}
        self.by_uid: Dict[str, SecSpec] = {}

    async def resolve(self, client: AsyncClient, *, figi: Optional[str]=None, uid: Optional[str]=None) -> SecSpec:
        if figi and figi in self.by_figi:
            return self.by_figi[figi]
        if uid and uid in self.by_uid:
            return self.by_uid[uid]

        if uid:
            resp = await client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID, id=uid)
        elif figi:
            resp = await client.instruments.get_instrument_by(
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=figi)
        else:
            raise ValueError("Need figi or uid")

        instr = resp.instrument
        is_future = instr.futures is not None or instr.instrument_type == "futures"

        fut_spec = None
        lot = instr.lot or 1
        if is_future:
            # Для фьючерсов возьмём подробности
            f = await client.instruments.get_future_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID, id=instr.uid)
            fut = f.instrument
            fut_spec = FutSpec(
                figi=fut.figi, uid=fut.uid, ticker=fut.ticker, class_code=fut.class_code,
                min_price_increment=float(fut.min_price_increment.units) + fut.min_price_increment.nano/1e9,
                min_price_increment_amount=float(fut.min_price_increment_amount.units) + fut.min_price_increment_amount.nano/1e9,
                lot=fut.lot or 1
            )
            lot = fut_spec.lot

        spec = SecSpec(
            figi=instr.figi, uid=instr.uid, ticker=instr.ticker, class_code=instr.class_code,
            lot=lot, currency=instr.currency, is_future=is_future, fut=fut_spec
        )
        self.by_figi[spec.figi] = spec
        self.by_uid[spec.uid] = spec
        return spec
