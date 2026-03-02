# Crypto Bot: Demo Grid Runner

## Что это
Проект для торговли в Binance Demo/Testnet режиме с сеточной стратегией.

Текущий фокус репозитория:
- запуск live demo-цикла по `bookTicker` (bid/ask);
- исполнение ордеров через Binance demo REST API;
- стратегия сетки (`GridBacktestAdapter` + `GridCore`);
- сохранение/восстановление runtime state;
- JSONL-телеметрия в `logs/`.

## Структура
- `src/run_demo.py` — основной раннер (entrypoint).
- `src/demo_execution.py` — demo execution adapter (REST/polling/fills).
- `src/binance_ws.py` — WS поток `bookTicker`.
- `src/strategy/grid_backtest_adapter.py` — стратегия/адаптер ордеров.
- `src/strategy/grid_core.py` — чистая механика сетки.
- `src/strategy/grid_types.py` — типы и состояние стратегии.
- `src/strategy/grid_paper_adapter.py` — сборка synthetic 15m свечей из тиков.

## Быстрый старт

### 1) Установка
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Настройка `.env`
Минимально нужны demo-ключи:

```bash
BINANCE_DEMO_API_KEY=...
BINANCE_DEMO_API_SECRET=...
```

Опционально:

```bash
# По умолчанию раннер сам ставит эти значения, но можно переопределить.
BINANCE_DEMO_REST_BASE_URL=https://demo-api.binance.com
BINANCE_DEMO_WS_BASE_URL=wss://demo-stream.binance.com
```

### 3) Запуск
Прямой запуск:

```bash
./.venv/bin/python -m src.run_demo --symbol ETHUSDT --max-ticks 0 --log-level INFO
```

Через `make`:

```bash
make demo SYMBOL=ETHUSDT
make demo_once        # max-ticks=1
make demo_fresh       # удалить state и стартовать "с нуля"
```

## State и логи

- State файл (по умолчанию): `data/run_demo_grid_state.json`
- Логи/метрики: `logs/demo_grid_<symbol>_{quotes,trades,metrics}.jsonl`

Полезные переменные:
- `RUN_DEMO_GRID_STATE_FILE` — путь к state файлу.
- `RUN_DEMO_GRID_LOG_DIR` — директория логов.
- `RUN_DEMO_GRID_LOG_PREFIX` — префикс jsonl файлов.
- `HEARTBEAT_SEC` — частота heartbeat логов.

## Проверка импорта
Быстрая smoke-проверка модулей:

```bash
make smoke_imports
```
