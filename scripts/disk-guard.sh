#!/bin/sh
set -eu

CHECK_PATH=${CHECK_PATH:-/}
WARNING=${DISK_WARNING_PERCENT:-85}
STOP=${DISK_STOP_PERCENT:-95}
STATE_DIR=${STATE_DIR:-/var/lib/vk-research-collector}
STOP_FILE="$STATE_DIR/disk-stop"
USAGE=$(df -P "$CHECK_PATH" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')

notify() {
  [ "${TELEGRAM_ENABLED:-false}" = "true" ] || return 0
  [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ] || return 0
  curl --fail --silent --show-error --max-time 10 \
    --data-urlencode "chat_id=$TELEGRAM_CHAT_ID" --data-urlencode "text=$1" \
    "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" >/dev/null
}

mkdir -p "$STATE_DIR"
if [ "$USAGE" -ge "$STOP" ]; then
  : > "$STOP_FILE"
  notify "VK Research Collector: диск заполнен на ${USAGE}%. Создание тяжёлых заданий должно быть приостановлено."
  echo "Критическое заполнение диска: ${USAGE}%" >&2
  exit 2
fi
rm -f "$STOP_FILE"
if [ "$USAGE" -ge "$WARNING" ]; then
  find /tmp -xdev -type f -mtime +1 -delete 2>/dev/null || true
  find /var/log -xdev -type f \( -name '*.gz' -o -name '*.old' \) -mtime +7 -delete 2>/dev/null || true
  notify "VK Research Collector: предупреждение, диск заполнен на ${USAGE}%. Удалены только временные файлы и старые ротируемые логи."
  echo "Предупреждение о заполнении диска: ${USAGE}%" >&2
fi
