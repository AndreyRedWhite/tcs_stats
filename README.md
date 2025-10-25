# tcs_stats

Асинхронный сборщик статистики по счёту Tinkoff Invest с агрегацией по оконным периодам и экспортом в Excel.

- **Python** ≥ 3.11
- **TZ по умолчанию:** `Europe/Moscow` (это прямо зафиксировано в коде)
- **Сетевой доступ:** только через `tinkoff.invest.AsyncClient` (без блокирующего I/O)
- **API:** актуальная курсорная пагинация `operations.get_operations_by_cursor(state=EXECUTED, with_trades=True)` — без устаревших методов
- **Модули раздельно:** сбор JSON (`tcs_stats/collect.py`) и экспорт JSON→Excel (`excel_export.py`)

## Установка

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
