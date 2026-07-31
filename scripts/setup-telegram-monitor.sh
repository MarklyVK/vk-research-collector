#!/usr/bin/env bash
set -Eeuo pipefail

umask 077
set +x

PROJECT_ROOT=${TELEGRAM_PROJECT_ROOT:-/opt/vk-research-collector}
MONITOR_USER=${TELEGRAM_MONITOR_USER:-deploy}
DRY_RUN=0

usage() {
  cat <<'EOF'
Использование: sudo ./scripts/setup-telegram-monitor.sh [--dry-run]

Token считывается без отображения и не передаётся аргументом процесса.
Перед запуском создайте бота через официальный @BotFather и отправьте ему /start.
EOF
}

die() {
  printf 'ОШИБКА: %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Неизвестный аргумент: $1" ;;
  esac
done

command -v python3 >/dev/null 2>&1 || die 'Не найдена команда python3.'
[[ -f "$PROJECT_ROOT/scripts/telegram-monitor.py" ]] \
  || die "Не найден $PROJECT_ROOT/scripts/telegram-monitor.py"
[[ -f "$PROJECT_ROOT/.env" ]] || die "Не найден $PROJECT_ROOT/.env"
id "$MONITOR_USER" >/dev/null 2>&1 || die "Не найден пользователь $MONITOR_USER."
if [[ "$PROJECT_ROOT" == /opt/* && "$(id -u)" -ne 0 ]]; then
  die 'Для production-записи запустите скрипт через sudo.'
fi

read -rsp "Telegram bot token: " TELEGRAM_BOT_TOKEN
printf '\n'
[[ -n "$TELEGRAM_BOT_TOKEN" ]] || die 'Token пуст.'

arguments=(
  "$PROJECT_ROOT/scripts/telegram-monitor.py"
  --setup-token-stdin
  --project-root "$PROJECT_ROOT"
  --owner "$MONITOR_USER"
)
if [[ "$DRY_RUN" -eq 1 ]]; then
  arguments+=(--dry-run)
fi

set +e
printf '%s\n' "$TELEGRAM_BOT_TOKEN" | python3 "${arguments[@]}"
status=$?
set -e
unset TELEGRAM_BOT_TOKEN
exit "$status"
