#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

RUNNER_USER=${RUNNER_USER:-deploy}
RUNNER_HOME=${RUNNER_HOME:-/opt/vk-research-collector/runner}
RUNNER_VERSION=${RUNNER_VERSION:-2.336.0}
RUNNER_SHA256=${RUNNER_SHA256:-04cf0be1aff4c3ec3554466c39124ca250e3effd8873bb7e8d68535aa9505d5d}
REPOSITORY_URL=${REPOSITORY_URL:-https://github.com/MarklyVK/vk-research-collector}
RUNNER_NAME=${RUNNER_NAME:-vk-collector-production}
RUNNER_LABELS=${RUNNER_LABELS:-production,vk-collector}

die() {
  printf 'ОШИБКА: %s\n' "$*" >&2
  exit 1
}

[[ $(id -u) -eq 0 ]] || die 'Запустите скрипт через sudo.'
[[ "$(uname -m)" == x86_64 ]] || die 'Поддерживается только Debian x64.'
grep -Eq '^ID=debian$' /etc/os-release || die 'Поддерживается Debian 12.'
grep -Eq '^VERSION_ID="?12"?$' /etc/os-release || die 'Требуется Debian 12.'
command -v curl >/dev/null || die 'Установите curl.'
command -v docker >/dev/null || die 'Установите Docker.'
docker compose version >/dev/null || die 'Установите Docker Compose plugin.'
id "$RUNNER_USER" >/dev/null 2>&1 || die "Сначала создайте системного пользователя $RUNNER_USER."
usermod -aG docker "$RUNNER_USER"

if [[ -z "${RUNNER_TOKEN:-}" ]]; then
  read -r -s -p 'Одноразовый GitHub registration token: ' RUNNER_TOKEN
  printf '\n'
fi
[[ -n "$RUNNER_TOKEN" ]] || die 'RUNNER_TOKEN пуст.'

install -d -o "$RUNNER_USER" -g "$RUNNER_USER" -m 700 "$RUNNER_HOME"
[[ ! -e "$RUNNER_HOME/.runner" ]] || die 'Runner уже зарегистрирован.'

archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
url="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
curl --fail --location --proto '=https' --tlsv1.2 --output "$archive" "$url"
printf '%s  %s\n' "$RUNNER_SHA256" "$archive" | sha256sum --check --status \
  || die 'SHA256 архива GitHub Runner не совпал.'
tar -xzf "$archive" -C "$RUNNER_HOME"
chown -R "$RUNNER_USER:$RUNNER_USER" "$RUNNER_HOME"

runuser -u "$RUNNER_USER" -- "$RUNNER_HOME/config.sh" \
  --unattended \
  --url "$REPOSITORY_URL" \
  --token "$RUNNER_TOKEN" \
  --name "$RUNNER_NAME" \
  --labels "$RUNNER_LABELS" \
  --work _work \
  --replace
unset RUNNER_TOKEN

cd "$RUNNER_HOME"
./svc.sh install "$RUNNER_USER"
./svc.sh start
systemctl enable "$(systemctl list-unit-files 'actions.runner*' --no-legend | awk 'NR==1 {print $1}')"
systemctl --no-pager status 'actions.runner*'
printf 'Runner установлен: %s; labels: self-hosted, linux, x64, %s\n' "$RUNNER_NAME" "$RUNNER_LABELS"
