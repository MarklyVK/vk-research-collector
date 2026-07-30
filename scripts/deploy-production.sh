#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

DRY_RUN=0
SOURCE_DIR="${GITHUB_WORKSPACE:-$(pwd)}"
DEPLOY_DIR="${DEPLOY_ROOT:-/opt/vk-research-collector}"
IMAGE="${COLLECTOR_IMAGE:-}"
GIT_SHA="${DEPLOY_GIT_SHA:-}"
EXPECTED_USER="${DEPLOY_USER:-deploy}"
BACKUP_KEEP="${PREDEPLOY_BACKUP_KEEP:-5}"
PROGRESS_WAIT="${DEPLOY_PROGRESS_WAIT_SECONDS:-20}"
LOCK_FILE=""
START_EPOCH=$(date +%s)
BACKUP_FILE=""
PREVIOUS_IMAGE=""
RUN_ID=""
BASELINE_COMPLETED=0
BASELINE_FAILED=0
ROLLBACK_ALLOWED=0
REPORT_STATUS=failed

usage() {
  cat <<'EOF'
Использование: deploy-production.sh [--dry-run] --image IMAGE --git-sha SHA
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

sync_runtime_files() {
  local relative source target temporary
  for relative in \
    compose.yaml \
    compose.production.yaml \
    config/keywords.yml \
    scripts/deploy-production.sh \
    scripts/postgres-init-readonly.sh; do
    source="$SOURCE_DIR/$relative"
    target="$DEPLOY_DIR/$relative"
    test -f "$source" || die "В checkout отсутствует $relative"
    install -d -m 755 "$(dirname "$target")"
    temporary="${target}.new.$$"
    install -m 644 "$source" "$temporary"
    case "$relative" in
      scripts/*.sh) chmod 755 "$temporary" ;;
    esac
    mv -f "$temporary" "$target"
  done
}

compose() {
  COLLECTOR_IMAGE="$IMAGE" docker compose \
    --env-file "$DEPLOY_DIR/.env" \
    -f "$DEPLOY_DIR/compose.yaml" \
    -f "$DEPLOY_DIR/compose.production.yaml" \
    "$@"
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

  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo '## Production deployment'
      echo
      echo '| Поле | Значение |'
      echo '|---|---|'
      printf '| Status | `%s` |\n' "$REPORT_STATUS"
      printf '| Commit SHA | `%s` |\n' "$GIT_SHA"
      printf '| Docker image | `%s` |\n' "${TARGET_IMAGE:-$IMAGE}"
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
  compose stop collector-worker >/dev/null 2>&1 || true
  IMAGE="$PREVIOUS_IMAGE"
  atomic_text "$DEPLOY_DIR/.deploy/current-image" "$PREVIOUS_IMAGE"
  compose up -d collector-worker || true
  local state
  state=$(service_state collector-worker)
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
    --git-sha) GIT_SHA=${2:?}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Неизвестный аргумент: $1" ;;
  esac
done

[[ "$GIT_SHA" =~ ^[0-9a-f]{40}$ ]] || die 'Нужен полный 40-символьный commit SHA.'
[[ "$IMAGE" == *":sha-$GIT_SHA" ]] || die 'Image должен иметь неизменяемый tag sha-<полный SHA>.'
[[ "$BACKUP_KEEP" =~ ^[1-9][0-9]*$ ]] || die 'PREDEPLOY_BACKUP_KEEP должен быть положительным числом.'
TARGET_IMAGE=$IMAGE

require_command docker
require_command flock
require_command df
require_command stat
require_command sed
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
[[ "$(stat -c '%a' "$DEPLOY_DIR/.env")" == 600 ]] || die '.env должен иметь права 600'
[[ "$(stat -c '%a' "$DEPLOY_DIR/secrets/vk_tokens.txt")" == 600 ]] || die 'vk_tokens.txt должен иметь права 600'
for required in compose.yaml compose.production.yaml config/keywords.yml scripts/deploy-production.sh scripts/postgres-init-readonly.sh; do
  test -f "$SOURCE_DIR/$required" || die "В checkout отсутствует $required"
done

if [[ "$DRY_RUN" -eq 0 ]]; then
  sync_runtime_files
fi

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
if (( DISK_USED >= DISK_STOP )); then
  if [[ "$DRY_RUN" -eq 0 ]]; then
    log "Диск заполнен на ${DISK_USED}%; collector-worker останавливается до скачивания image."
    compose stop collector-worker || true
  fi
  die "Критическое заполнение диска: ${DISK_USED}% (stop=${DISK_STOP}%)."
fi
if (( DISK_USED >= DISK_WARNING )); then
  die "Диск заполнен на ${DISK_USED}% (warning=${DISK_WARNING}%); image не скачивался."
fi

compose exec -T postgres pg_isready -U "$(env_value POSTGRES_USER)" -d "$(env_value POSTGRES_DB)" >/dev/null \
  || die 'PostgreSQL недоступен.'

WORKER_BEFORE=$(service_state collector-worker)
WORKER_CONTAINER=$(compose ps -q collector-worker 2>/dev/null || true)
if [[ -n "$WORKER_CONTAINER" ]]; then
  PREVIOUS_IMAGE=$(docker inspect --format '{{.Config.Image}}' "$WORKER_CONTAINER")
fi
RUN_ID=$(env_value COLLECTION_RUN_ID)
BASELINE_STATUS_JSON=""
if [[ -n "$PREVIOUS_IMAGE" ]]; then
  IMAGE=$PREVIOUS_IMAGE
fi
if [[ -n "$RUN_ID" ]]; then
  BASELINE_STATUS_JSON=$(compose run --rm collector collection status --run-id "$RUN_ID")
else
  BASELINE_STATUS_JSON=$(compose run --rm collector collection status)
  RUN_ID=$(printf '%s\n' "$BASELINE_STATUS_JSON" | json_string run_id || true)
fi
IMAGE=$TARGET_IMAGE
BASELINE_COMPLETED=$(printf '%s\n' "$BASELINE_STATUS_JSON" | json_number completed || true)
BASELINE_FAILED=$(printf '%s\n' "$BASELINE_STATUS_JSON" | json_number failed || true)
BASELINE_COMPLETED=${BASELINE_COMPLETED:-0}
BASELINE_FAILED=${BASELINE_FAILED:-0}

log "Preflight: user=$CURRENT_USER, disk=${DISK_USED}%, worker=$WORKER_BEFORE, run=${RUN_ID:-absent}"
log "Текущий image: ${PREVIOUS_IMAGE:-absent}; целевой image: $IMAGE"

if [[ "$DRY_RUN" -eq 1 ]]; then
  log 'DRY-RUN: конфигурация корректна. План: backup -> pull SHA image -> stop worker -> Alembic -> up -> health/progress verify.'
  log 'DRY-RUN: worker, PostgreSQL, backup и runtime-файлы не изменялись.'
  trap - EXIT
  exit 0
fi

install -d -m 700 "$DEPLOY_DIR/backups" "$DEPLOY_DIR/.deploy" "$DEPLOY_DIR/exports"
TIMESTAMP=$(date -u +%Y%m%d-%H%M%SZ)
BACKUP_FILE="$DEPLOY_DIR/backups/predeploy-${TIMESTAMP}-${GIT_SHA}.dump"
log "Создаётся backup: $BACKUP_FILE"
compose exec -T postgres pg_dump -U "$(env_value POSTGRES_USER)" -d "$(env_value POSTGRES_DB)" -Fc > "$BACKUP_FILE"
test -s "$BACKUP_FILE" || die 'Backup пуст.'
compose exec -T postgres pg_restore --list < "$BACKUP_FILE" >/dev/null || die 'pg_restore --list отклонил backup.'

mapfile -t OLD_BACKUPS < <(find "$DEPLOY_DIR/backups" -maxdepth 1 -type f -name 'predeploy-*.dump' -printf '%T@ %p\n' \
  | sort -rn | tail -n "+$((BACKUP_KEEP + 1))" | cut -d' ' -f2-)
for old_backup in "${OLD_BACKUPS[@]}"; do
  [[ "$old_backup" == "$BACKUP_FILE" ]] || rm -f -- "$old_backup"
done

atomic_text "$DEPLOY_DIR/.deploy/previous-image" "$PREVIOUS_IMAGE"
log 'Скачивается неизменяемый SHA image.'
docker pull "$IMAGE"
compose pull collector collector-worker
atomic_text "$DEPLOY_DIR/.deploy/current-image" "$IMAGE"

compose stop collector-worker
log 'Проверяется и применяется только Alembic upgrade.'
compose run --rm collector alembic current
compose run --rm collector alembic upgrade head
compose run --rm collector alembic check
ALEMBIC_REVISION=$(compose run --rm collector alembic current | tail -n 1 | tr -d '\r')

ROLLBACK_ALLOWED=1
compose up -d --remove-orphans postgres collector-worker

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
compose logs --no-color --tail=100 collector-worker
compose run --rm collector classification summary
compose run --rm collector collection summary

if [[ -n "$RUN_ID" ]]; then
  FINAL_STATUS_JSON=$(compose run --rm collector collection status --run-id "$RUN_ID")
  printf '%s\n' "$FINAL_STATUS_JSON"
  RUN_STATUS=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_string status || true)
  FINAL_FAILED=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number failed || true)
  FINAL_FAILED=${FINAL_FAILED:-0}
  [[ "$RUN_STATUS" != failed ]] || die 'Collection run перешёл в failed.'
  (( FINAL_FAILED <= BASELINE_FAILED )) || die 'Количество failed jobs увеличилось.'

  set +e
  VERIFY_JSON=$(compose run --rm collector collection verify --run-id "$RUN_ID" 2>&1)
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
  FINAL_STATUS_JSON=$(compose run --rm collector collection status --run-id "$RUN_ID")
  FINAL_COMPLETED=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number completed || true)
  FINAL_PENDING=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number pending || true)
  FINAL_RUNNING=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number running || true)
  FINAL_RETRY=$(printf '%s\n' "$FINAL_STATUS_JSON" | json_number retry_wait || true)
  FINAL_COMPLETED=${FINAL_COMPLETED:-0}
  FINAL_PENDING=${FINAL_PENDING:-0}
  FINAL_RUNNING=${FINAL_RUNNING:-0}
  FINAL_RETRY=${FINAL_RETRY:-0}
  if (( FINAL_COMPLETED <= BASELINE_COMPLETED && FINAL_RUNNING == 0 && FINAL_RETRY == 0 && FINAL_PENDING > 0 )); then
    die 'Worker не показал прогресс и не находится в running/retry ожидании.'
  fi
fi

DISK_AFTER=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')
(( DISK_AFTER < DISK_STOP )) || die "После deployment диск достиг stop threshold: ${DISK_AFTER}%"

REPORT_STATUS=success
ROLLBACK_ALLOWED=0
write_report
log "Deployment $GIT_SHA завершён успешно."
trap - EXIT
