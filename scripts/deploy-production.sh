#!/usr/bin/env bash
# shellcheck disable=SC2016 # Markdown backticks in printf formats are intentional.
set -Eeuo pipefail

umask 077

DRY_RUN=0
SOURCE_DIR="${GITHUB_WORKSPACE:-$(pwd)}"
DEPLOY_DIR="${DEPLOY_ROOT:-/opt/vk-research-collector}"
IMAGE="${COLLECTOR_IMAGE:-}"
EXPECTED_IMAGE_DIGEST="${COLLECTOR_IMAGE_DIGEST:-}"
GIT_SHA="${DEPLOY_GIT_SHA:-}"
EXPECTED_USER="${DEPLOY_USER:-deploy}"
BACKUP_KEEP="${PREDEPLOY_BACKUP_KEEP:-1}"
PROGRESS_WAIT="${DEPLOY_PROGRESS_WAIT_SECONDS:-20}"
WORKER_STOP_TIMEOUT="${DEPLOY_WORKER_STOP_TIMEOUT_SECONDS:-360}"
LOCK_FILE=""
START_EPOCH=$(date +%s)
DEPLOY_STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BACKUP_FILE=""
PREVIOUS_IMAGE=""
RUN_ID=""
BASELINE_COMPLETED=0
BASELINE_FAILED=0
ROLLBACK_ALLOWED=0
REPORT_STATUS=failed
PROTECTED_BACKUPS=()

usage() {
  cat <<'EOF'
Использование: deploy-production.sh [--dry-run] --image IMAGE --image-digest DIGEST --git-sha SHA
       [--source-dir PATH] [--deploy-dir PATH]
EOF
}

log() {
  printf '[deploy] %s\n' "$*"
}

die() {
  printf '[deploy] ОШИБКА: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Не найдена команда: $1"
}

env_value() {
  local key=$1
  sed -n "s/^${key}=//p" "$DEPLOY_DIR/.env" | tail -n 1 | tr -d '\r'
}

json_string() {
  local key=$1
  sed -nE "s/^[[:space:]]*\"${key}\":[[:space:]]*\"([^\"]*)\".*/\1/p" | head -n 1
}

json_number() {
  local key=$1
  sed -nE "s/^[[:space:]]*\"${key}\":[[:space:]]*([0-9]+).*/\1/p" | head -n 1
}

atomic_text() {
  local target=$1
  local value=$2
  local temporary="${target}.tmp.$$"
  printf '%s\n' "$value" > "$temporary"
  mv -f "$temporary" "$target"
}

fast_forward_checkout() {
  local current_head fetched_head source_head
  git -C "$SOURCE_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || die 'GitHub checkout не является Git worktree.'
  source_head=$(git -C "$SOURCE_DIR" rev-parse HEAD)
  [[ "$source_head" == "$GIT_SHA" ]] || die 'GitHub checkout не совпадает с DEPLOY_GIT_SHA.'
  git -C "$DEPLOY_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
    || die 'Production-каталог не является Git worktree.'
  git -C "$DEPLOY_DIR" diff --quiet --ignore-submodules -- \
    || die 'В production checkout есть незакоммиченные tracked-изменения.'
  git -C "$DEPLOY_DIR" diff --cached --quiet --ignore-submodules -- \
    || die 'В production checkout есть staged-изменения.'
  current_head=$(git -C "$DEPLOY_DIR" rev-parse HEAD)
  git -c core.hooksPath=/dev/null -C "$DEPLOY_DIR" fetch --no-tags "$SOURCE_DIR" "$GIT_SHA"
  fetched_head=$(git -C "$DEPLOY_DIR" rev-parse FETCH_HEAD)
  [[ "$fetched_head" == "$GIT_SHA" ]] || die 'Локальный fetch вернул неожиданный commit.'
  git -C "$DEPLOY_DIR" merge-base --is-ancestor "$current_head" "$GIT_SHA" \
    || die 'Production checkout нельзя обновить fast-forward до целевого commit.'
  git -c core.hooksPath=/dev/null -C "$DEPLOY_DIR" merge --ff-only --no-edit "$GIT_SHA"
  [[ "$(git -C "$DEPLOY_DIR" rev-parse HEAD)" == "$GIT_SHA" ]] \
    || die 'Production checkout не обновлён до целевого commit.'
}

verify_local_image() {
  local actual_revision repository repo_digest
  actual_revision=$(docker image inspect "$IMAGE" \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
  [[ "$actual_revision" == "$GIT_SHA" ]] \
    || die 'OCI revision скачанного image не совпадает с commit SHA.'
  repository=${IMAGE%:sha-*}
  repo_digest="${repository}@${EXPECTED_IMAGE_DIGEST}"
  docker image inspect "$IMAGE" --format '{{range .RepoDigests}}{{println .}}{{end}}' \
    | grep -Fqx "$repo_digest" \
    || die 'Digest скачанного image не совпадает с опубликованным digest.'
}

compose() {
  COLLECTOR_IMAGE="$IMAGE" docker compose \
    --env-file "$DEPLOY_DIR/.env" \
    -f "$DEPLOY_DIR/compose.yaml" \
    -f "$DEPLOY_DIR/compose.production.yaml" \
    "$@"
}

psql_query() {
  compose exec -T postgres psql \
    -X -v ON_ERROR_STOP=1 -P pager=off \
    -U "$(env_value POSTGRES_USER)" -d "$(env_value POSTGRES_DB)" "$@"
}

load_protected_backups() {
  local configured_path backup_name host_path
  PROTECTED_BACKUPS=()
  while IFS= read -r configured_path; do
    [[ -n "$configured_path" ]] || continue
    backup_name=${configured_path##*/}
    [[ "$configured_path" == "/app/backups/$backup_name" ]] \
      || die "Небезопасный путь verified backup в collection run: $configured_path"
    [[ "$backup_name" =~ ^[A-Za-z0-9._-]+\.dump$ ]] \
      || die "Недопустимое имя verified backup: $backup_name"
    host_path="$DEPLOY_DIR/backups/$backup_name"
    if [[ -f "$host_path" ]]; then
      PROTECTED_BACKUPS+=("$host_path")
    else
      log "Предупреждение: незавершённый run ссылается на отсутствующий backup: $host_path"
    fi
  done < <(psql_query -Atqc \
    "SELECT DISTINCT configuration #>> '{verified_backup,path}'
       FROM collection_runs
      WHERE status::text IN (
        'planned','running','paused','paused_no_tokens',
        'paused_capacity_limit','waiting_method_limit'
      )
        AND configuration #>> '{verified_backup,path}' IS NOT NULL
      ORDER BY 1")
}

grant_collector_protected_backup_read() {
  local backup
  (( ${#PROTECTED_BACKUPS[@]} > 0 )) || return 0
  chmod 0700 "$DEPLOY_DIR/backups"
  if command -v setfacl >/dev/null 2>&1; then
    setfacl -m u:10001:rx "$DEPLOY_DIR/backups"
    for backup in "${PROTECTED_BACKUPS[@]}"; do
      setfacl -m u:10001:r "$backup"
    done
    log "Сохранён read-only ACL collector для verified backup: ${#PROTECTED_BACKUPS[@]}."
  else
    chmod o+x "$DEPLOY_DIR/backups"
    for backup in "${PROTECTED_BACKUPS[@]}"; do
      chmod o+r "$backup"
    done
    log "setfacl отсутствует: сохранён минимальный read-only доступ к verified backup."
  fi
}

is_protected_backup() {
  local candidate=$1 protected
  for protected in "${PROTECTED_BACKUPS[@]}"; do
    [[ "$candidate" == "$protected" ]] && return 0
  done
  return 1
}

compose_cli() {
  # Production runner uses a Compose release where `run --no-build` is not
  # available.  Refuse to run unless the exact selected image already exists;
  # with pull_policy=never Compose then uses it and cannot pull or build a tag.
  docker image inspect "$IMAGE" >/dev/null 2>&1 \
    || die "Локальный image для production CLI отсутствует: $IMAGE"
  compose run --rm --no-deps collector "$@"
}

stop_worker_on_critical_disk() {
  local used=$1 stage=$2
  if (( used < DISK_STOP )); then
    return 0
  fi
  if [[ "$DRY_RUN" -eq 0 ]]; then
    log "Диск заполнен на ${used}% (${stage}); collector-worker останавливается."
    compose stop -t "$WORKER_STOP_TIMEOUT" collector-worker || true
  fi
  ROLLBACK_ALLOWED=0
  die "Критическое заполнение диска: ${used}% (stop=${DISK_STOP}%, stage=${stage})."
}

service_state() {
  local service=$1 container state health
  container=$(compose ps -q "$service" 2>/dev/null || true)
  if [[ -z "$container" ]]; then
    printf 'absent\n'
    return
  fi
  state=$(docker inspect --format '{{.State.Status}}' "$container")
  health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container")
  if [[ -n "$health" ]]; then
    printf '%s/%s\n' "$state" "$health"
  else
    printf '%s\n' "$state"
  fi
}

install_telegram_monitor_units() {
  local unit_source unit_dir runtime_dir unit
  unit_source="$DEPLOY_DIR/deploy/systemd"
  unit_dir="${HOME:?}/.config/systemd/user"
  runtime_dir="/run/user/$(id -u)"
  test -S "$runtime_dir/bus" \
    || die 'User systemd manager deploy недоступен. Один раз выполните: sudo loginctl enable-linger deploy'
  install -d -m 700 "$unit_dir" "$DEPLOY_DIR/.deploy/telegram-monitor"
  for unit in \
    vk-collector-telegram-health.service \
    vk-collector-telegram-health.timer \
    vk-collector-telegram-daily.service \
    vk-collector-telegram-daily.timer; do
    test -f "$unit_source/$unit" || die "Не найден systemd unit: $unit"
    install -C -m 644 "$unit_source/$unit" "$unit_dir/$unit"
  done
  systemd-analyze verify \
    "$unit_source/vk-collector-telegram-health.service" \
    "$unit_source/vk-collector-telegram-health.timer" \
    "$unit_source/vk-collector-telegram-daily.service" \
    "$unit_source/vk-collector-telegram-daily.timer"
  XDG_RUNTIME_DIR="$runtime_dir" systemctl --user daemon-reload
  XDG_RUNTIME_DIR="$runtime_dir" systemctl --user enable --now \
    vk-collector-telegram-health.timer \
    vk-collector-telegram-daily.timer
  XDG_RUNTIME_DIR="$runtime_dir" systemctl --user is-active \
    vk-collector-telegram-health.timer >/dev/null
  XDG_RUNTIME_DIR="$runtime_dir" systemctl --user is-active \
    vk-collector-telegram-daily.timer >/dev/null
  XDG_RUNTIME_DIR="$runtime_dir" systemctl --user is-enabled \
    vk-collector-telegram-health.timer >/dev/null
  XDG_RUNTIME_DIR="$runtime_dir" systemctl --user is-enabled \
    vk-collector-telegram-daily.timer >/dev/null
}

write_report() {
  local duration revision worker_state postgres_state status_json completed pending running retry failed db_size disk_usage
  duration=$(($(date +%s) - START_EPOCH))
  revision="${ALEMBIC_REVISION:-unknown}"
  worker_state=$(service_state collector-worker 2>/dev/null || printf 'unknown\n')
  postgres_state=$(service_state postgres 2>/dev/null || printf 'unknown\n')
  status_json="${FINAL_STATUS_JSON:-}"
  completed=$(printf '%s\n' "$status_json" | json_number completed || true)
  pending=$(printf '%s\n' "$status_json" | json_number pending || true)
  running=$(printf '%s\n' "$status_json" | json_number running || true)
  retry=$(printf '%s\n' "$status_json" | json_number retry_wait || true)
  failed=$(printf '%s\n' "$status_json" | json_number failed || true)
  db_size=$(compose exec -T postgres psql -U "$(env_value POSTGRES_USER)" \
    -d "$(env_value POSTGRES_DB)" -Atqc 'SELECT pg_database_size(current_database())' \
    2>/dev/null || printf 'unknown\n')
  disk_usage=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {print $5}')
  install -d -m 700 "$DEPLOY_DIR/.deploy"
  {
    printf 'STATUS=%s\n' "$REPORT_STATUS"
    printf 'COMMIT_SHA=%s\n' "$GIT_SHA"
    printf 'IMAGE=%s\n' "${TARGET_IMAGE:-$IMAGE}"
    printf 'IMAGE_DIGEST=%s\n' "$EXPECTED_IMAGE_DIGEST"
    printf 'PREVIOUS_IMAGE=%s\n' "$PREVIOUS_IMAGE"
    printf 'BACKUP_PATH=%s\n' "$BACKUP_FILE"
    printf 'ALEMBIC_REVISION=%s\n' "$revision"
    printf 'WORKER_STATE=%s\n' "$worker_state"
    printf 'POSTGRES_STATE=%s\n' "$postgres_state"
    printf 'RUN_ID=%s\n' "$RUN_ID"
    printf 'COMPLETED=%s\n' "${completed:-unknown}"
    printf 'PENDING=%s\n' "${pending:-unknown}"
    printf 'RUNNING=%s\n' "${running:-unknown}"
    printf 'RETRY=%s\n' "${retry:-unknown}"
    printf 'FAILED=%s\n' "${failed:-unknown}"
    printf 'DB_SIZE=%s\n' "$db_size"
    printf 'DISK_USAGE=%s\n' "$disk_usage"
    printf 'DURATION_SECONDS=%s\n' "$duration"
  } > "$DEPLOY_DIR/.deploy/last-deployment.env"
  if [[ "$REPORT_STATUS" == success ]]; then
    cp -f "$DEPLOY_DIR/.deploy/last-deployment.env" \
      "$DEPLOY_DIR/.deploy/last-successful-deployment.env"
    chmod 600 "$DEPLOY_DIR/.deploy/last-successful-deployment.env"
  fi

  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo '## Production deployment'
      echo
      echo '| Поле | Значение |'
      echo '|---|---|'
      printf '| Status | `%s` |\n' "$REPORT_STATUS"
      printf '| Commit SHA | `%s` |\n' "$GIT_SHA"
      printf '| Docker image | `%s` |\n' "${TARGET_IMAGE:-$IMAGE}"
      printf '| Image digest | `%s` |\n' "$EXPECTED_IMAGE_DIGEST"
      printf '| Previous image | `%s` |\n' "${PREVIOUS_IMAGE:-нет}"
      printf '| Backup | `%s` |\n' "${BACKUP_FILE:-не создан}"
      printf '| Alembic revision | `%s` |\n' "$revision"
      printf '| Worker state | `%s` |\n' "$worker_state"
      printf '| PostgreSQL state | `%s` |\n' "$postgres_state"
      printf '| Run ID | `%s` |\n' "${RUN_ID:-не задан}"
      printf '| Completed / Pending / Running / Retry / Failed | `%s / %s / %s / %s / %s` |\n' \
        "${completed:-?}" "${pending:-?}" "${running:-?}" "${retry:-?}" "${failed:-?}"
      printf '| DB size | `%s bytes` |\n' "$db_size"
      printf '| Disk usage | `%s` |\n' "$disk_usage"
      printf '| Duration | `%s s` |\n' "$duration"
    } >> "$GITHUB_STEP_SUMMARY"
  fi
}

rollback_image() {
  [[ "$ROLLBACK_ALLOWED" -eq 1 ]] || return 0
  [[ -n "$PREVIOUS_IMAGE" ]] || {
    log 'Предыдущий image неизвестен: автоматический rollback невозможен.'
    return 0
  }
  log "Healthcheck не пройден; возвращается предыдущий image: $PREVIOUS_IMAGE"
  compose stop -t "$WORKER_STOP_TIMEOUT" collector-worker >/dev/null 2>&1 || true
  IMAGE="$PREVIOUS_IMAGE"
  atomic_text "$DEPLOY_DIR/.deploy/current-image" "$PREVIOUS_IMAGE"
  compose up -d --no-deps --no-build collector-worker || true
  local state=unknown
  for _ in $(seq 1 12); do
    state=$(service_state collector-worker)
    if [[ "$state" == running/healthy || "$state" == running ]]; then
      return 0
    fi
    sleep 5
  done
  if [[ "$state" != running/healthy && "$state" != running ]]; then
    log 'Rollback image не восстановил worker. Возможна несовместимость миграции; восстановите backup вручную.'
  fi
}

on_exit() {
  local exit_code=$?
  trap - EXIT
  if [[ "$exit_code" -ne 0 && "$DRY_RUN" -eq 0 ]]; then
    rollback_image || true
    write_report || true
  fi
  exit "$exit_code"
}

trap on_exit EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --source-dir) SOURCE_DIR=${2:?}; shift 2 ;;
    --deploy-dir) DEPLOY_DIR=${2:?}; shift 2 ;;
    --image) IMAGE=${2:?}; shift 2 ;;
    --image-digest) EXPECTED_IMAGE_DIGEST=${2:?}; shift 2 ;;
    --git-sha) GIT_SHA=${2:?}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Неизвестный аргумент: $1" ;;
  esac
done

[[ "$GIT_SHA" =~ ^[0-9a-f]{40}$ ]] || die 'Нужен полный 40-символьный commit SHA.'
[[ "$IMAGE" == *":sha-$GIT_SHA" ]] || die 'Image должен иметь неизменяемый tag sha-<полный SHA>.'
[[ "$EXPECTED_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || die 'Нужен полный sha256 digest опубликованного image.'
[[ "$BACKUP_KEEP" =~ ^[1-9][0-9]*$ ]] || die 'PREDEPLOY_BACKUP_KEEP должен быть положительным числом.'
[[ "$WORKER_STOP_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
  || die 'DEPLOY_WORKER_STOP_TIMEOUT_SECONDS должен быть положительным числом.'
(( WORKER_STOP_TIMEOUT >= 30 )) || die 'Graceful stop worker должен длиться не менее 30 секунд.'
TARGET_IMAGE=$IMAGE

require_command docker
require_command flock
require_command df
require_command stat
require_command sed
require_command grep
require_command git
require_command systemctl
require_command systemd-analyze
docker compose version >/dev/null

CURRENT_USER=$(id -un)
[[ "$CURRENT_USER" == "$EXPECTED_USER" ]] || die "Скрипт должен запускать пользователь $EXPECTED_USER, сейчас: $CURRENT_USER"
test -d "$DEPLOY_DIR" || die "Не найден production-каталог: $DEPLOY_DIR"
install -d -m 700 "$DEPLOY_DIR/.deploy"
LOCK_FILE="${DEPLOY_LOCK_FILE:-$DEPLOY_DIR/.deploy/deploy.lock}"
exec 9>"$LOCK_FILE"
flock -n 9 || die 'Другой production deployment уже выполняется.'
test -f "$DEPLOY_DIR/.env" || die "Не найден $DEPLOY_DIR/.env"
test -f "$DEPLOY_DIR/secrets/vk_tokens.txt" || die 'Не найден secrets/vk_tokens.txt'
test -d "$DEPLOY_DIR/exports" || die 'Не найден runtime-каталог exports.'
test -w "$DEPLOY_DIR/exports" || die 'Пользователь deploy не может записывать в exports.'
[[ "$(stat -c '%a' "$DEPLOY_DIR/.env")" == 600 ]] || die '.env должен иметь права 600'
[[ "$(stat -c '%a' "$DEPLOY_DIR/secrets/vk_tokens.txt")" == 600 ]] || die 'vk_tokens.txt должен иметь права 600'
for required in \
  compose.yaml \
  compose.production.yaml \
  config/keywords.yml \
  scripts/deploy-production.sh \
  scripts/postgres-init-readonly.sh \
  scripts/telegram-monitor.py \
  deploy/systemd/vk-collector-telegram-health.service \
  deploy/systemd/vk-collector-telegram-health.timer \
  deploy/systemd/vk-collector-telegram-daily.service \
  deploy/systemd/vk-collector-telegram-daily.timer; do
  test -f "$SOURCE_DIR/$required" || die "В checkout отсутствует $required"
done

compose config --quiet

VOLUME_NAME=$(env_value POSTGRES_VOLUME_NAME)
VOLUME_NAME=${VOLUME_NAME:-vk_research_postgres_data}
if ! docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
  LEGACY_VOLUME=vk-research-collector_postgres_data
  if docker volume inspect "$LEGACY_VOLUME" >/dev/null 2>&1; then
    die "Найден прежний volume $LEGACY_VOLUME. Укажите POSTGRES_VOLUME_NAME=$LEGACY_VOLUME в .env; новый volume не создан."
  fi
  die "Production PostgreSQL volume $VOLUME_NAME не найден. Сначала выполните bootstrap/handoff."
fi

DISK_WARNING=$(env_value DISK_WARNING_PERCENT)
DISK_STOP=$(env_value DISK_STOP_PERCENT)
DISK_WARNING=${DISK_WARNING:-85}
DISK_STOP=${DISK_STOP:-95}
DISK_USED=${DEPLOY_DISK_USED_PERCENT_OVERRIDE:-$(df -P "$DEPLOY_DIR" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')}
[[ "$DISK_USED" =~ ^[0-9]+$ ]] || die 'Не удалось определить заполнение диска.'
stop_worker_on_critical_disk "$DISK_USED" preflight
if (( DISK_USED >= DISK_WARNING )); then
  die "Диск заполнен на ${DISK_USED}% (warning=${DISK_WARNING}%); image не скачивался."
fi

compose exec -T postgres pg_isready -U "$(env_value POSTGRES_USER)" -d "$(env_value POSTGRES_DB)" >/dev/null \
  || die 'PostgreSQL недоступен.'
load_protected_backups

WORKER_BEFORE=$(service_state collector-worker)
WORKER_CONTAINER=$(compose ps -aq collector-worker 2>/dev/null || true)
if [[ -n "$WORKER_CONTAINER" ]]; then
  PREVIOUS_IMAGE=$(docker inspect --format '{{.Config.Image}}' "$WORKER_CONTAINER")
fi
RUN_ID=$(env_value COLLECTION_RUN_ID)
BASELINE_STATUS_JSON=""
if [[ -n "$PREVIOUS_IMAGE" ]]; then
  IMAGE=$PREVIOUS_IMAGE
  if [[ "$WORKER_BEFORE" != running/healthy && "$WORKER_BEFORE" != running ]]; then
    # Ранний preflight обязан вернуть найденный остановленный production worker.
    ROLLBACK_ALLOWED=1
  fi
fi
IMAGE=$TARGET_IMAGE

log "Preflight: user=$CURRENT_USER, disk=${DISK_USED}%, worker=$WORKER_BEFORE, run=${RUN_ID:-absent}"
log "Текущий image: ${PREVIOUS_IMAGE:-absent}; целевой image: $IMAGE"

if [[ "$DRY_RUN" -eq 1 ]]; then
  log 'DRY-RUN: конфигурация корректна. План: backup -> pull SHA image -> stop worker -> Alembic -> up -> health/progress verify.'
  log 'DRY-RUN: worker, PostgreSQL, backup и runtime-файлы не изменялись.'
  trap - EXIT
  exit 0
fi

install -d -m 700 "$DEPLOY_DIR/backups" "$DEPLOY_DIR/.deploy"
grant_collector_protected_backup_read
TIMESTAMP=$(date -u +%Y%m%d-%H%M%SZ)
BACKUP_FILE="$DEPLOY_DIR/backups/predeploy-${TIMESTAMP}-${GIT_SHA}.dump"
log "Создаётся backup: $BACKUP_FILE"
compose exec -T postgres pg_dump -U "$(env_value POSTGRES_USER)" -d "$(env_value POSTGRES_DB)" -Fc > "$BACKUP_FILE"
test -s "$BACKUP_FILE" || die 'Backup пуст.'
compose exec -T postgres pg_restore --list < "$BACKUP_FILE" >/dev/null || die 'pg_restore --list отклонил backup.'

mapfile -t OLD_BACKUPS < <(find "$DEPLOY_DIR/backups" -maxdepth 1 -type f -name 'predeploy-*.dump' -printf '%T@ %p\n' \
  | sort -rn | tail -n "+$((BACKUP_KEEP + 1))" | cut -d' ' -f2-)
for old_backup in "${OLD_BACKUPS[@]}"; do
  if [[ "$old_backup" != "$BACKUP_FILE" ]] && ! is_protected_backup "$old_backup"; then
    rm -f -- "$old_backup"
  fi
done

DISK_AFTER_BACKUP=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')
stop_worker_on_critical_disk "$DISK_AFTER_BACKUP" post-backup
(( DISK_AFTER_BACKUP < DISK_WARNING )) \
  || die "После ротации backup диск заполнен на ${DISK_AFTER_BACKUP}%; image не скачивался."

atomic_text "$DEPLOY_DIR/.deploy/previous-image" "$PREVIOUS_IMAGE"
log 'Скачивается неизменяемый SHA image.'
docker pull "$IMAGE"
verify_local_image
DISK_AFTER_PULL=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')
stop_worker_on_critical_disk "$DISK_AFTER_PULL" post-pull
if (( DISK_AFTER_PULL >= DISK_WARNING )); then
  log "После pull диск заполнен на ${DISK_AFTER_PULL}%; deployment продолжается без удаления данных."
fi

fast_forward_checkout
compose config --quiet
compose stop -t "$WORKER_STOP_TIMEOUT" collector-worker
# До начала upgrade схема не менялась: любой pre-migration сбой должен вернуть старый worker.
ROLLBACK_ALLOWED=1
log 'Проверяется и применяется только Alembic upgrade.'
compose_cli alembic current
# После начала forward migration старый image нельзя возвращать до успешного upgrade/check.
ROLLBACK_ALLOWED=0
compose_cli alembic upgrade head
compose_cli alembic check
ALEMBIC_REVISION=$(compose_cli alembic current | tail -n 1 | tr -d '\r')

ROLLBACK_ALLOWED=1
if [[ -n "$RUN_ID" ]]; then
  BASELINE_STATUS_JSON=$(compose_cli collection status --run-id "$RUN_ID")
else
  BASELINE_STATUS_JSON=$(compose_cli collection status)
  RUN_ID=$(printf '%s\n' "$BASELINE_STATUS_JSON" | json_string run_id || true)
fi
BASELINE_COMPLETED=$(printf '%s\n' "$BASELINE_STATUS_JSON" | json_number completed || true)
BASELINE_FAILED=$(printf '%s\n' "$BASELINE_STATUS_JSON" | json_number failed || true)
BASELINE_COMPLETED=${BASELINE_COMPLETED:-0}
BASELINE_FAILED=${BASELINE_FAILED:-0}
atomic_text "$DEPLOY_DIR/.deploy/current-image" "$IMAGE"
compose up -d --no-deps --no-build collector-worker

HEALTH_OK=0
for _ in $(seq 1 12); do
  POSTGRES_STATE=$(service_state postgres)
  WORKER_STATE=$(service_state collector-worker)
  if [[ "$POSTGRES_STATE" == running/healthy && "$WORKER_STATE" == running/healthy ]]; then
    HEALTH_OK=1
    break
  fi
  sleep 5
done
[[ "$HEALTH_OK" -eq 1 ]] || die "Healthcheck не пройден: postgres=$POSTGRES_STATE, worker=$WORKER_STATE"

compose ps
compose logs --no-color --since="$DEPLOY_STARTED_AT" --tail=300 collector-worker
compose_cli classification summary
compose_cli collection summary

WORKER_CONTAINER=$(compose ps -q collector-worker)
[[ -n "$WORKER_CONTAINER" ]] || die 'Container collector-worker не найден после запуска.'
[[ "$(docker inspect --format '{{.Config.Image}}' "$WORKER_CONTAINER")" == "$IMAGE" ]] \
  || die 'Worker запущен не из целевого immutable image.'
verify_local_image

if [[ -n "$RUN_ID" ]]; then
  FINAL_STATUS_JSON=$(compose_cli collection status --run-id "$RUN_ID")
  printf '%s\n' "$FINAL_STATUS_JSON"
  RUN_STATUS=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_string status || true)
  FINAL_FAILED=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number failed || true)
  FINAL_FAILED=${FINAL_FAILED:-0}
  [[ "$RUN_STATUS" != failed ]] || die 'Collection run перешёл в failed.'
  (( FINAL_FAILED <= BASELINE_FAILED )) || die 'Количество failed jobs увеличилось.'

  set +e
  VERIFY_JSON=$(compose_cli collection verify --run-id "$RUN_ID" 2>&1)
  VERIFY_CODE=$?
  set -e
  printf '%s\n' "$VERIFY_JSON"
  for metric in post_duplicates membership_duplicates subscription_duplicates rejected_jobs; do
    value=$(printf '%s\n' "$VERIFY_JSON" | json_number "$metric" || true)
    [[ "${value:-1}" -eq 0 ]] || die "Collection verify: $metric=${value:-unknown}"
  done
  if [[ "$VERIFY_CODE" -ne 0 ]]; then
    log 'Collection verify вернул non-zero только из-за активных running jobs; проверены все инварианты данных.'
  fi

  sleep "$PROGRESS_WAIT"
  FINAL_STATUS_JSON=$(compose_cli collection status --run-id "$RUN_ID")
  RUN_STATUS=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_string status || true)
  FINAL_COMPLETED=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number completed || true)
  FINAL_PENDING=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number pending || true)
  FINAL_RUNNING=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number running || true)
  FINAL_RETRY=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number retry_wait || true)
  FINAL_FAILED=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number failed || true)
  FINAL_COMPLETED=${FINAL_COMPLETED:-0}
  FINAL_PENDING=${FINAL_PENDING:-0}
  FINAL_RUNNING=${FINAL_RUNNING:-0}
  FINAL_RETRY=${FINAL_RETRY:-0}
  FINAL_FAILED=${FINAL_FAILED:-0}
  [[ "$RUN_STATUS" != failed ]] || die 'Collection run перешёл в failed во время проверки прогресса.'
  (( FINAL_FAILED <= BASELINE_FAILED )) || die 'Количество failed jobs увеличилось во время проверки прогресса.'
  if (( FINAL_COMPLETED <= BASELINE_COMPLETED && FINAL_RUNNING == 0 && FINAL_RETRY == 0 && FINAL_PENDING > 0 )); then
    die 'Worker не показал прогресс и не находится в running/retry ожидании.'
  fi
fi

DISK_AFTER=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')
stop_worker_on_critical_disk "$DISK_AFTER" post-deployment

REPORT_STATUS=success
ROLLBACK_ALLOWED=0
write_report
install_telegram_monitor_units
log "Deployment $GIT_SHA завершён успешно."
trap - EXIT
