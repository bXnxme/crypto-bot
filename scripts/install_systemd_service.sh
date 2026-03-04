#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-crypto-bot}"
REPO_DIR="${REPO_DIR:-/opt/crypto-bot}"
RUN_USER="${RUN_USER:-${SUDO_USER:-$(id -un)}}"
RUN_GROUP="${RUN_GROUP:-$(id -gn "$RUN_USER")}"

UNIT_SRC="${REPO_DIR}/deploy/systemd/crypto-bot.service"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}.service"
RUNNER_ENV_SRC="${REPO_DIR}/deploy/systemd/crypto-bot-runner.env.example"
RUNNER_ENV_DST="/etc/default/crypto-bot-runner"

if [[ "$EUID" -ne 0 ]]; then
  echo "Run as root (sudo)." >&2
  exit 1
fi

if [[ ! -d "${REPO_DIR}" ]]; then
  echo "Repository directory not found: ${REPO_DIR}" >&2
  exit 1
fi

if [[ ! -f "${UNIT_SRC}" ]]; then
  echo "Missing unit template: ${UNIT_SRC}" >&2
  exit 1
fi

if [[ ! -f "${REPO_DIR}/.env" ]]; then
  echo "Missing ${REPO_DIR}/.env (required for API keys)." >&2
  exit 1
fi

if [[ ! -x "${REPO_DIR}/.venv/bin/python" ]]; then
  echo "Missing virtualenv python at ${REPO_DIR}/.venv/bin/python" >&2
  echo "Create it first: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

sed \
  -e "s|__WORKDIR__|${REPO_DIR}|g" \
  -e "s|__USER__|${RUN_USER}|g" \
  -e "s|__GROUP__|${RUN_GROUP}|g" \
  "${UNIT_SRC}" > "${UNIT_DST}"

if [[ ! -f "${RUNNER_ENV_DST}" ]]; then
  if [[ -f "${RUNNER_ENV_SRC}" ]]; then
    cp "${RUNNER_ENV_SRC}" "${RUNNER_ENV_DST}"
  else
    cat > "${RUNNER_ENV_DST}" <<EOF
SYMBOL=ETHUSDT
INTERVAL=1m
LOG_LEVEL=INFO
EOF
  fi
fi

chown "${RUN_USER}:${RUN_GROUP}" "${RUNNER_ENV_DST}" || true

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"
systemctl --no-pager --full status "${SERVICE_NAME}" || true

echo
echo "Installed systemd service: ${SERVICE_NAME}"
echo "Edit runtime params in: ${RUNNER_ENV_DST}"
echo "Restart service: sudo systemctl restart ${SERVICE_NAME}"
echo "Tail logs: sudo journalctl -u ${SERVICE_NAME} -f"
