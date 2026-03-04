# --- Tooling ---
# Prefer venv python/pip if present.
PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python)
PIP := $(if $(wildcard .venv/bin/pip),.venv/bin/pip,pip)

# --- Demo runner params ---
SYMBOL ?= ETHUSDT
INTERVAL ?= 15m
MAX_TICKS ?= 0
LOG_LEVEL ?= INFO
EXTRA ?=
STATE_FILE ?= data/run_demo_grid_state.json

RUN_DEMO_ARGS := --symbol $(SYMBOL) --interval $(INTERVAL) --max-ticks $(MAX_TICKS) --log-level $(LOG_LEVEL) $(EXTRA)

.PHONY: help venv install demo demo_once demo_fresh demo_state clean_logs smoke_imports

help:
	@echo "Targets:"
	@echo "  make venv                  # create .venv"
	@echo "  make install               # install deps (pip -r requirements.txt)"
	@echo "  make demo SYMBOL=ETHUSDT INTERVAL=15m  # run demo grid loop (MAX_TICKS=0 means infinite)"
	@echo "  make demo_once             # run one tick cycle (MAX_TICKS=1)"
	@echo "  make demo_fresh            # remove saved state then run demo"
	@echo "  make demo_state            # print current state file path/content (if exists)"
	@echo "  make clean_logs            # delete demo JSONL logs from logs/"
	@echo "  make smoke_imports         # quick import smoke check"
	@echo ""
	@echo "Required env vars:"
	@echo "  BINANCE_DEMO_API_KEY, BINANCE_DEMO_API_SECRET"
	@echo ""
	@echo "Useful overrides:"
	@echo "  SYMBOL=..., INTERVAL=..., MAX_TICKS=..., LOG_LEVEL=..., EXTRA='...'"
	@echo "  STATE_FILE=... (used by demo_fresh/demo_state)"

venv:
	@test -d .venv || python -m venv .venv
	@echo "Created .venv"

install:
	$(PIP) install -r requirements.txt

demo:
	$(PYTHON) -m src.run_demo $(RUN_DEMO_ARGS)

demo_once:
	$(MAKE) demo MAX_TICKS=1

demo_fresh:
	@rm -f $(STATE_FILE)
	$(MAKE) demo

demo_state:
	@echo "State file: $(STATE_FILE)"
	@if [ -f "$(STATE_FILE)" ]; then cat "$(STATE_FILE)"; else echo "State file does not exist"; fi

clean_logs:
	@rm -f logs/demo_grid_*_quotes.jsonl logs/demo_grid_*_trades.jsonl logs/demo_grid_*_metrics.jsonl
	@echo "Deleted demo JSONL logs (if existed)"

smoke_imports:
	$(PYTHON) -c "import importlib; mods=['src.run_demo','src.demo_execution','src.binance_ws','src.strategy.grid_core','src.strategy.grid_types','src.strategy.grid_backtest_adapter','src.strategy.grid_paper_adapter']; [importlib.import_module(m) and print('ok', m) for m in mods]"
