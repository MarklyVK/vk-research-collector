#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

APPLY=0
DEPLOY_DIR="${DEPLOY_ROOT:-/opt/vk-research-collector}"
EXPECTED_USER="${DEPLOY_USER:-deploy}"
EXPECTED_DEPLOY_DIR="${CLEANUP_EXPECTED_DEPLOY_DIR:-/opt/vk-research-collector}"
PROJECT_LABEL="${CLEANUP_COMPOSE_PROJECT:-vk-research-collector}"
COLLECTOR_REPOSITORY="ghcr.io/marklyvk/vk-research-collector/collector"

usage() {
  cat <<'EOF'
Использование: cleanup-production-storage.sh [--apply] [--deploy-dir PATH]

Без --apply выполняется только preview. Скрипт всегда сохраняет последний проверенный
PostgreSQL dump, текущий image, rollback image, volume, exports и secrets.
EOF
}

log() {
  printf '[storage-cleanup] %s\n' "$*"
}

die() {
  printf '[storage-cleanup] ОШИБКА: %s\n' "$*" >&2
  exit 1
}

env_value() {
  local key=$1
  sed -n "s/^${key}=//p" "$DEPLOY_DIR/.env" | tail -n 1 | tr -d '\r'
}

compose() {
  COLLECTOR_IMAGE="$CURRENT_IMAGE" docker compose \
    --env-file "$DEPLOY_DIR/.env" \
    -f "$DEPLOY_DIR/compose.yaml" \
    -f "$DEPLOY_DIR/compose.production.yaml" \
    "$@"
}

service_state() {
  local service=$1 container state health
  container=$(compose ps -q "$service" 2>/dev/null || true)
  [[ -n "$container" ]] || {
    printf 'absent\n'
    return
  }
  state=$(docker inspect --format '{{.State.Status}}' "$container")
  health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container")
  if [[ -n "$health" ]]; then
    printf '%s/%s\n' "$state" "$health"
  else
    printf '%s\n' "$state"
  fi
}

file_size() {
  stat -c '%s' "$1"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY=1; shift ;;
    --deploy-dir) DEPLOY_DIR=${2:?}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Неизвестный аргумент: $1" ;;
  esac
done

for command in docker flock find sort stat realpath df du awk sed grep; do
  command -v "$command" >/dev/null 2>&1 || die "Не найдена команда: $command"
done
docker compose version >/dev/null

[[ "$(id -un)" == "$EXPECTED_USER" ]] \
  || die "Скрипт должен запускать пользователь $EXPECTED_USER"
DEPLOY_DIR=$(realpath -e "$DEPLOY_DIR")
EXPECTED_DEPLOY_DIR=$(realpath -e "$EXPECTED_DEPLOY_DIR")
[[ "$DEPLOY_DIR" == "$EXPECTED_DEPLOY_DIR" ]] \
  || die "Разрешена очистка только $EXPECTED_DEPLOY_DIR, получено: $DEPLOY_DIR"
test -f "$DEPLOY_DIR/.env" || die 'Не найден production .env'
test -f "$DEPLOY_DIR/.deploy/current-image" || die 'Не найден .deploy/current-image'
test -d "$DEPLOY_DIR/backups" || die 'Не найден каталог backups'
test -d "$DEPLOY_DIR/exports" || die 'Не найден каталог exports'
test -d "$DEPLOY_DIR/secrets" || die 'Не найден каталог secrets'

exec 9>"$DEPLOY_DIR/.deploy/deploy.lock"
flock -n 9 || die 'Другой production deployment или cleanup уже выполняется.'

CURRENT_IMAGE=$(tr -d '\r\n' < "$DEPLOY_DIR/.deploy/current-image")
PREVIOUS_IMAGE=$(tr -d '\r\n' < "$DEPLOY_DIR/.deploy/previous-image" 2>/dev/null || true)
[[ "$CURRENT_IMAGE" == "$COLLECTOR_REPOSITORY:sha-"* ]] \
  || die 'current-image не является immutable collector SHA image'
docker image inspect "$CURRENT_IMAGE" >/dev/null 2>&1 \
  || die "Текущий image отсутствует локально: $CURRENT_IMAGE"
CURRENT_IMAGE_ID=$(docker image inspect --format '{{.Id}}' "$CURRENT_IMAGE")
PREVIOUS_IMAGE_ID=""
if [[ -n "$PREVIOUS_IMAGE" ]] && docker image inspect "$PREVIOUS_IMAGE" >/dev/null 2>&1; then
  PREVIOUS_IMAGE_ID=$(docker image inspect --format '{{.Id}}' "$PREVIOUS_IMAGE")
fi

compose config --quiet
POSTGRES_STATE_BEFORE=$(service_state postgres)
WORKER_STATE_BEFORE=$(service_state collector-worker)
[[ "$POSTGRES_STATE_BEFORE" == running/healthy ]] \
  || die "PostgreSQL не healthy до cleanup: $POSTGRES_STATE_BEFORE"
[[ "$WORKER_STATE_BEFORE" == running/healthy ]] \
  || die "Worker не healthy до cleanup: $WORKER_STATE_BEFORE"
WORKER_CONTAINER=$(compose ps -q collector-worker)
[[ "$(docker inspect --format '{{.Config.Image}}' "$WORKER_CONTAINER")" == "$CURRENT_IMAGE" ]] \
  || die 'Worker запущен не из current-image'

POSTGRES_USER=$(env_value POSTGRES_USER)
POSTGRES_DB=$(env_value POSTGRES_DB)
POSTGRES_USER=${POSTGRES_USER:-vk_collector}
POSTGRES_DB=${POSTGRES_DB:-vk_research}
REVISION=$(compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -Atqc 'SELECT version_num FROM alembic_version')
[[ "$REVISION" == 20260810_0007 ]] || die "Неожиданная Alembic revision: $REVISION"

BACKUP_DIR="$DEPLOY_DIR/backups"
PROTECTED_BACKUPS=()
while IFS= read -r configured_path; do
  [[ -n "$configured_path" ]] || continue
  backup_name=${configured_path##*/}
  [[ "$configured_path" == "/app/backups/$backup_name" ]] \
    || die "Небезопасный путь verified backup в collection run: $configured_path"
  [[ "$backup_name" =~ ^[A-Za-z0-9._-]+\.dump$ ]] \
    || die "Недопустимое имя verified backup: $backup_name"
  protected_path="$BACKUP_DIR/$backup_name"
  [[ -f "$protected_path" ]] && PROTECTED_BACKUPS+=("$protected_path")
done < <(compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -P pager=off \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc \
  "SELECT DISTINCT configuration #>> '{verified_backup,path}'
     FROM collection_runs
    WHERE status::text IN (
      'planned','running','paused','paused_no_tokens',
      'paused_capacity_limit','waiting_method_limit'
    )
      AND configuration #>> '{verified_backup,path}' IS NOT NULL
    ORDER BY 1")
mapfile -d '' -t BACKUP_RECORDS < <(
  find "$BACKUP_DIR" -type f -name '*.dump' -printf '%T@ %p\0' | sort -z -nr
)
(( ${#BACKUP_RECORDS[@]} > 0 )) || die 'Нет ни одного PostgreSQL dump для сохранения'
LATEST_BACKUP=${BACKUP_RECORDS[0]#* }
[[ "$LATEST_BACKUP" == "$BACKUP_DIR/"* ]] || die 'Последний backup вышел за разрешённый каталог'
test -s "$LATEST_BACKUP" || die 'Последний backup пуст'
compose exec -T postgres pg_restore --list < "$LATEST_BACKUP" >/dev/null \
  || die 'Последний backup не прошёл pg_restore --list'
LATEST_BACKUP_BYTES=$(file_size "$LATEST_BACKUP")

mapfile -d '' -t ALL_BACKUP_FILES < <(find "$BACKUP_DIR" -type f -print0)
BACKUP_DELETE=()
BACKUP_DELETE_BYTES=0
for path in "${ALL_BACKUP_FILES[@]}"; do
  [[ "$path" == "$LATEST_BACKUP" ]] && continue
  protected=0
  for protected_path in "${PROTECTED_BACKUPS[@]}"; do
    if [[ "$path" == "$protected_path" ]]; then
      protected=1
      break
    fi
  done
  (( protected == 1 )) && continue
  resolved=$(realpath -m "$path")
  [[ "$resolved" == "$BACKUP_DIR/"* ]] || die "Backup path вышел за allowlist: $resolved"
  BACKUP_DELETE+=("$resolved")
  size=$(file_size "$resolved")
  BACKUP_DELETE_BYTES=$((BACKUP_DELETE_BYTES + size))
done

STOPPED_CONTAINERS=()
while IFS= read -r container; do
  [[ -n "$container" ]] || continue
  service=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.service"}}' "$container")
  [[ "$service" == postgres || "$service" == collector-worker ]] && continue
  STOPPED_CONTAINERS+=("$container")
done < <(docker ps -aq \
  --filter "label=com.docker.compose.project=$PROJECT_LABEL" \
  --filter status=exited)

IMAGE_DELETE=()
IMAGE_DELETE_BYTES=0
while read -r repository tag image_id; do
  [[ -n "$repository" && -n "$tag" && -n "$image_id" ]] || continue
  case "$repository" in
    "$COLLECTOR_REPOSITORY"|vk-research-collector-collector) ;;
    *) continue ;;
  esac
  [[ "$tag" == '<none>' ]] && continue
  full_id=$(docker image inspect --format '{{.Id}}' "$image_id")
  [[ "$full_id" == "$CURRENT_IMAGE_ID" || "$full_id" == "$PREVIOUS_IMAGE_ID" ]] && continue
  reference="$repository:$tag"
  IMAGE_DELETE+=("$reference")
  size=$(docker image inspect --format '{{.Size}}' "$image_id")
  IMAGE_DELETE_BYTES=$((IMAGE_DELETE_BYTES + size))
done < <(docker image ls --format '{{.Repository}} {{.Tag}} {{.ID}}')

mapfile -t TEMP_DELETE < <(
  find "$DEPLOY_DIR/.deploy" -maxdepth 1 -type f -name '*.tmp.*' -mtime +0 -print
)

DISK_USED_BEFORE=$(df -B1 --output=used "$DEPLOY_DIR" | awk 'NR==2 {print $1}')
DISK_AVAILABLE_BEFORE=$(df -B1 --output=avail "$DEPLOY_DIR" | awk 'NR==2 {print $1}')
DISK_PERCENT_BEFORE=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {print $5}')

log "Режим: $([[ "$APPLY" -eq 1 ]] && printf apply || printf preview)"
log "Alembic: $REVISION; PostgreSQL: $POSTGRES_STATE_BEFORE; worker: $WORKER_STATE_BEFORE"
log "Сохраняется последний backup: $LATEST_BACKUP ($LATEST_BACKUP_BYTES bytes)"
log "Verified backup незавершённых запусков под защитой: ${#PROTECTED_BACKUPS[@]}"
log "Старых backup-файлов к удалению: ${#BACKUP_DELETE[@]} ($BACKUP_DELETE_BYTES bytes)"
for path in "${BACKUP_DELETE[@]}"; do
  log "DELETE backup: $path ($(file_size "$path") bytes)"
done
log "Старых collector image references к удалению: ${#IMAGE_DELETE[@]}"
for reference in "${IMAGE_DELETE[@]}"; do
  log "DELETE image: $reference"
done
log "Остановленных одноразовых контейнеров проекта к удалению: ${#STOPPED_CONTAINERS[@]}"
log "Старых временных deployment-файлов к удалению: ${#TEMP_DELETE[@]}"
log 'Docker disk usage до cleanup:'
docker system df

if [[ "$APPLY" -eq 1 ]]; then
  for path in "${BACKUP_DELETE[@]}"; do
    rm -f -- "$path"
  done
  find "$BACKUP_DIR" -depth -mindepth 1 -type d -empty -delete
  for path in "${TEMP_DELETE[@]}"; do
    resolved=$(realpath -m "$path")
    [[ "$resolved" == "$DEPLOY_DIR/.deploy/"* ]] || die "Temp path вышел за allowlist: $resolved"
    rm -f -- "$resolved"
  done
  if (( ${#STOPPED_CONTAINERS[@]} > 0 )); then
    docker container rm "${STOPPED_CONTAINERS[@]}"
  fi
  for reference in "${IMAGE_DELETE[@]}"; do
    docker image rm "$reference"
  done
  docker image prune -f
  docker builder prune -af
  sync
fi

POSTGRES_STATE_AFTER=$(service_state postgres)
WORKER_STATE_AFTER=$(service_state collector-worker)
[[ "$POSTGRES_STATE_AFTER" == running/healthy ]] \
  || die "PostgreSQL не healthy после cleanup: $POSTGRES_STATE_AFTER"
[[ "$WORKER_STATE_AFTER" == running/healthy ]] \
  || die "Worker не healthy после cleanup: $WORKER_STATE_AFTER"
test -s "$LATEST_BACKUP" || die 'Сохранённый последний backup исчез после cleanup'
docker image inspect "$CURRENT_IMAGE" >/dev/null 2>&1 \
  || die 'Текущий image исчез после cleanup'
if [[ -n "$PREVIOUS_IMAGE_ID" ]]; then
  docker image inspect "$PREVIOUS_IMAGE" >/dev/null 2>&1 \
    || die 'Rollback image исчез после cleanup'
fi

DISK_USED_AFTER=$(df -B1 --output=used "$DEPLOY_DIR" | awk 'NR==2 {print $1}')
DISK_AVAILABLE_AFTER=$(df -B1 --output=avail "$DEPLOY_DIR" | awk 'NR==2 {print $1}')
DISK_PERCENT_AFTER=$(df -P "$DEPLOY_DIR" | awk 'NR==2 {print $5}')
FREED_BYTES=$((DISK_USED_BEFORE - DISK_USED_AFTER))

log 'Docker disk usage после cleanup/preview:'
docker system df
printf 'MODE=%s\n' "$([[ "$APPLY" -eq 1 ]] && printf apply || printf preview)"
printf 'LATEST_BACKUP=%s\n' "$LATEST_BACKUP"
printf 'LATEST_BACKUP_BYTES=%s\n' "$LATEST_BACKUP_BYTES"
printf 'BACKUP_DELETE_COUNT=%s\n' "${#BACKUP_DELETE[@]}"
printf 'BACKUP_DELETE_BYTES=%s\n' "$BACKUP_DELETE_BYTES"
printf 'IMAGE_DELETE_COUNT=%s\n' "${#IMAGE_DELETE[@]}"
printf 'IMAGE_DELETE_BYTES=%s\n' "$IMAGE_DELETE_BYTES"
printf 'STOPPED_CONTAINER_DELETE_COUNT=%s\n' "${#STOPPED_CONTAINERS[@]}"
printf 'TEMP_DELETE_COUNT=%s\n' "${#TEMP_DELETE[@]}"
printf 'DISK_USED_BEFORE=%s\n' "$DISK_USED_BEFORE"
printf 'DISK_USED_AFTER=%s\n' "$DISK_USED_AFTER"
printf 'DISK_AVAILABLE_BEFORE=%s\n' "$DISK_AVAILABLE_BEFORE"
printf 'DISK_AVAILABLE_AFTER=%s\n' "$DISK_AVAILABLE_AFTER"
printf 'DISK_PERCENT_BEFORE=%s\n' "$DISK_PERCENT_BEFORE"
printf 'DISK_PERCENT_AFTER=%s\n' "$DISK_PERCENT_AFTER"
printf 'FREED_BYTES=%s\n' "$FREED_BYTES"
printf 'ALEMBIC_REVISION=%s\n' "$REVISION"
printf 'WORKER_STATE=%s\n' "$WORKER_STATE_AFTER"
printf 'POSTGRES_STATE=%s\n' "$POSTGRES_STATE_AFTER"
