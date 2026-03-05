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
- `src/telegram_bot.py` — Telegram control bot (polling + команды управления раннером).
- `src/telegram_miniapp.py` — backend для Telegram Mini App (aiohttp + API).
- `src/demo_execution.py` — demo execution adapter (REST/polling/fills).
- `src/binance_ws.py` — WS поток `bookTicker`.
- `src/strategy/grid_backtest_adapter.py` — стратегия/адаптер ордеров.
- `src/strategy/grid_core.py` — чистая механика сетки.
- `src/strategy/grid_types.py` — типы и состояние стратегии.
- `src/strategy/grid_paper_adapter.py` — сборка synthetic свечей из тиков для заданного интервала.
- `webapp/` — фронтенд Telegram Mini App (`index.html`, `app.js`, `styles.css`).

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

Стабильность/безопасность (опционально):

```bash
# open_orders diff handling
RUN_DEMO_GRID_OPEN_ORDERS_DIFF_GRACE_SEC=6
RUN_DEMO_GRID_OPEN_ORDERS_DIFF_STRATEGY_CANCEL=0

# adapter guards
RUN_DEMO_GRID_ENABLE_LOT_RECOVERY=0
RUN_DEMO_GRID_BUY_SPEND_MIN_RATIO=0.20
RUN_DEMO_GRID_STRICT_SPEND_PCT=1
```

S3-бэкапы (опционально, для state/logs):

```bash
RUN_DEMO_GRID_S3_ENABLED=1
RUN_DEMO_GRID_S3_ENDPOINT=https://s3.twcstorage.ru
RUN_DEMO_GRID_S3_REGION=ru-1
RUN_DEMO_GRID_S3_BUCKET=<bucket_name>
RUN_DEMO_GRID_S3_ACCESS_KEY=<access_key>
RUN_DEMO_GRID_S3_SECRET_KEY=<secret_key>
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
make telegram         # запустить Telegram control bot
make miniapp          # запустить backend Telegram Mini App
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
- `RUN_DEMO_GRID_OPEN_ORDERS_DIFF_GRACE_SEC` — grace (сек) перед diff-cancel.
- `RUN_DEMO_GRID_OPEN_ORDERS_DIFF_STRATEGY_CANCEL` — прокидывать diff-cancel в стратегию (`0/1`).
- `RUN_DEMO_GRID_ENABLE_LOT_RECOVERY` — включить runtime lot recovery (`0/1`).
- `RUN_DEMO_GRID_BUY_SPEND_MIN_RATIO` — минимальная доля spend для BUY (anti-dust guard).
- `RUN_DEMO_GRID_STRICT_SPEND_PCT` — строгий `% spend` для BUY: полный размер или skip (`1`, по умолчанию).
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота.
- `TELEGRAM_ALLOWED_USER_IDS` — whitelist user id через запятую (пусто = без фильтра).
- `TELEGRAM_RUNNER_SERVICE` — имя systemd сервиса раннера (по умолчанию `crypto-bot`).
- `TELEGRAM_POLL_TIMEOUT_SEC`, `TELEGRAM_CMD_TIMEOUT_SEC`, `TELEGRAM_LOG_LINES`, `TELEGRAM_DEBUG`.
- `TELEGRAM_SYNC_MENU_COMMANDS` — синхронизировать меню команд (`setMyCommands`, `0/1`).
- `TELEGRAM_SYNC_MENU_BUTTON` — синхронизировать кнопку меню бота (`setChatMenuButton`, `0/1`).
- `TELEGRAM_MENU_BUTTON_MODE` — режим кнопки меню: `commands` (показать список команд), `web_app` (открывать Mini App), `default`.
- `TELEGRAM_MENU_BUTTON_TEXT` — текст кнопки меню (`Mini App` по умолчанию).
- `TELEGRAM_NOTIFY_TRADES` — включить уведомления о `fill` сделках (`0/1`).
- `TELEGRAM_NOTIFY_CHAT_IDS` — chat id для уведомлений (пусто => `TELEGRAM_ALLOWED_USER_IDS`).
- `TELEGRAM_NOTIFY_SYMBOL` — символ лог-файла для уведомлений (пусто => авто).
- `TELEGRAM_NOTIFY_POLL_SEC` — частота опроса trade-лога.
- `TELEGRAM_NOTIFY_FROM_BEGINNING` — читать trade-лог с начала (`1`) или только новые события (`0`).
- `TELEGRAM_NOTIFY_ENRICH_BINANCE` — догружать `qty/price/fee/maker` из Binance `myTrades` по `order_id`, если в логе не хватает полей (`0/1`).
- `TELEGRAM_NOTIFY_ENRICH_RECV_WINDOW_MS` — `recvWindow` для signed Binance-запросов.
- `TELEGRAM_NOTIFY_ENRICH_CACHE_MAX` — размер кэша обогащенных ордеров (уменьшает повторные API-вызовы).
- `TELEGRAM_MINIAPP_URL` — публичный HTTPS URL mini app (для команды `/app` в Telegram).
- `TELEGRAM_MINIAPP_HOST`, `TELEGRAM_MINIAPP_PORT` — bind mini app сервера.
- `TELEGRAM_MINIAPP_ALLOW_RUNNER_CONTROL` — разрешить start/stop/restart через mini app (`0/1`).
- `TELEGRAM_MINIAPP_AUTH_MAX_AGE_SEC` — TTL для проверки Telegram `initData`.
- `TELEGRAM_MINIAPP_DEV_MODE` — локальная отладка без Telegram (`0/1`), на сервере держать `0`.
- `TELEGRAM_MINIAPP_DEV_USER_ID` — user id для dev-режима.
- `TELEGRAM_MINIAPP_TIMEZONE` — часовой пояс для отображения времени и расчета `PnL за день` (по умолчанию `Europe/Moscow`).

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

## Telegram Bot (MVP)

Запуск:

```bash
./.venv/bin/python -m src.telegram_bot
# или
make telegram
```

Поддерживаемые команды:
- `/start`, `/help`
- `/ping`
- `/status` (service + state summary + open orders side/price/qty)
- `/runner`
- `/app` (кнопка открытия Telegram Mini App)
- `/notify` (текущая конфигурация уведомлений)
- `/start_runner`, `/stop_runner`, `/restart_runner`
- `/logs [N]` (journalctl tail)

### systemd для Telegram бота

Шаблон:
- `deploy/systemd/crypto-bot-telegram.service`

Пример установки на сервер:

```bash
sudo cp deploy/systemd/crypto-bot-telegram.service /etc/systemd/system/crypto-bot-telegram.service
sudo sed -i "s|__USER__|root|g; s|__GROUP__|root|g; s|__WORKDIR__|/opt/crypto-bot|g" /etc/systemd/system/crypto-bot-telegram.service
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-bot-telegram
sudo systemctl status crypto-bot-telegram --no-pager
```

## Telegram Mini App (MVP)

Что умеет mini app:
- показывает статус runner-сервиса и snapshot state;
- показывает последние `fill`-сделки из `logs/*_trades.jsonl`;
- умеет `start/stop/restart` runner (если `TELEGRAM_MINIAPP_ALLOW_RUNNER_CONTROL=1`).

Локальный запуск:

```bash
make miniapp
# или
./.venv/bin/python -m src.telegram_miniapp
```

Роуты:
- `GET /` — UI mini app.
- `GET /health` — healthcheck.
- `GET /api/status` — статус сервиса + state.
- `GET /api/trades?limit=20` — последние fill-сделки.
- `POST /api/runner/{start|stop|restart}` — управление раннером.

Безопасность:
- `/api/*` принимает только валидный Telegram WebApp `initData` (HMAC проверка через `TELEGRAM_BOT_TOKEN`).
- Дополнительно применяется `TELEGRAM_ALLOWED_USER_IDS` (если задан).
- Для локальной отладки можно включить `TELEGRAM_MINIAPP_DEV_MODE=1`.

### systemd для Mini App

Шаблоны:
- `deploy/systemd/crypto-bot-miniapp.service`
- `deploy/systemd/crypto-bot-miniapp.env.example`

Пример установки на сервер:

```bash
sudo cp deploy/systemd/crypto-bot-miniapp.service /etc/systemd/system/crypto-bot-miniapp.service
sudo sed -i "s|__USER__|root|g; s|__GROUP__|root|g; s|__WORKDIR__|/opt/crypto-bot|g" /etc/systemd/system/crypto-bot-miniapp.service
sudo cp deploy/systemd/crypto-bot-miniapp.env.example /etc/default/crypto-bot-miniapp
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-bot-miniapp
sudo systemctl status crypto-bot-miniapp --no-pager
```
