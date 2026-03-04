# Crypto Bot: Demo Grid Runner

Буду рад, если для Вас данное решение окажется полезным и также буду рад любым пожертвованиям голодающему поволжью -
- USDT ERC20 - 0x1647B1773578AbBa725b2405B7c8Ad76de550F19
- USDT SOL - 6yqkL3nVcxJQ2Ekd6g8M4hG4qaRBKf6ZU8j3oWWYL6uX
- USDT TON - UQBktYrKNpWFQhg2vPPLl5ky_5CPe7DOIrGvHkegM6TKqgQR
- USDT TRC20 - TN8vagbvvWDDxaAiwqGfCB49RTV6aGtqyp

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
- `src/strategy/grid_paper_adapter.py` — сборка synthetic свечей из тиков для заданного интервала.

## Быстрый старт

### 1) Установка
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Настройка `.env`
Скопируй шаблон и заполни ключи:

```bash
cp .env.example .env
```

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

S3-бэкапы (опционально, для state/logs):

```bash
RUN_DEMO_GRID_S3_ENABLED=1
RUN_DEMO_GRID_S3_ENDPOINT=https://s3.twcstorage.ru
RUN_DEMO_GRID_S3_REGION=ru-1
RUN_DEMO_GRID_S3_BUCKET=1ddf608c-461a-4387-bcdb-b378a99c8075
RUN_DEMO_GRID_S3_ACCESS_KEY=S545TAWWV9PPA1R6CIPQ
RUN_DEMO_GRID_S3_SECRET_KEY=cUSfVMFfNBAdQ0LgRTVsfvHo6qCUi6R0q9HnSN9a
RUN_DEMO_GRID_S3_PREFIX=demo-grid
# 0 = не грузить jsonl логи периодически (только финальная выгрузка при остановке)
RUN_DEMO_GRID_S3_LOG_SYNC_SEC=0
```

### 3) Запуск
Прямой запуск:

```bash
./.venv/bin/python -m src.run_demo --symbol ETHUSDT --interval 15m --max-ticks 0 --log-level INFO
```

Через `make`:

```bash
make demo SYMBOL=ETHUSDT INTERVAL=15m
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
- `RUN_DEMO_GRID_INTERVAL` — интервал synthetic свечей (например `1m`, `5m`, `15m`, `1h`).
- `HEARTBEAT_SEC` — частота heartbeat логов.
- `RUN_DEMO_GRID_S3_ENABLED` — включить выгрузку бэкапов в S3 (`0/1`).
- `RUN_DEMO_GRID_S3_ENDPOINT`, `RUN_DEMO_GRID_S3_REGION`, `RUN_DEMO_GRID_S3_BUCKET` — endpoint/region/бакет.
- `RUN_DEMO_GRID_S3_ACCESS_KEY`, `RUN_DEMO_GRID_S3_SECRET_KEY` — ключи доступа.
- `RUN_DEMO_GRID_S3_PREFIX` — префикс пути в бакете (по умолчанию `demo-grid`).
- `RUN_DEMO_GRID_S3_LOG_SYNC_SEC` — период синхронизации JSONL логов в S3 (секунды, `0` = отключено).

Когда S3 включен:
- state загружается на каждый flush и при остановке раннера;
- `quotes/trades/metrics` загружаются при остановке раннера;
- при `RUN_DEMO_GRID_S3_LOG_SYNC_SEC > 0` логи дополнительно синхронизируются периодически.

## Проверка импорта
Быстрая smoke-проверка модулей:

```bash
make smoke_imports
```

## Безопасный push на GitHub

- `.env`, runtime-state (`data/run_demo_grid_state*.json*`) и логи (`logs/`) исключены из Git через `.gitignore`.
- В репозиторий коммитится только `.env.example` с плейсхолдерами.
- Перед публикацией полезно проверить индекс:

```bash
git ls-files | rg -n "^(\\.env|data/|logs/)"
```

Если реальные ключи когда-то попали в историю Git, перед публичным push обязательно ротируй их в Binance.
