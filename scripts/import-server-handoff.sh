#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

DUMP_FILE=${1:-}
MANIFEST_FILE=${2:-}
CONFIRM=${3:-}
DEPLOY_DIR=${DEPLOY_ROOT:-/opt/vk-research-collector}
PROGRESS_WAIT=${HANDOFF_PROGRESS_WAIT_SECONDS:-20}
REPORT_FILE=""

die() {
  printf '[handoff] ОШИБКА: %s\n' "$*" >&2
  exit 1
}

env_value() {
  local key=$1
  sed -n "s/^${key}=//p" "$DEPLOY_DIR/.env" | tail -n 1 | tr -d '\r'
}

compose() {
  local image
  image=$(cat "$DEPLOY_DIR/.deploy/current-image" 2>/dev/null || env_value COLLECTOR_IMAGE)
  [[ -n "$image" ]] || die 'Не задан COLLECTOR_IMAGE и отсутствует .deploy/current-image.'
  COLLECTOR_IMAGE="$image" docker compose \
    --env-file "$DEPLOY_DIR/.env" \
    -f "$DEPLOY_DIR/compose.yaml" \
    -f "$DEPLOY_DIR/compose.production.yaml" \
    "$@"
}

json_number() {
  local key=$1
  sed -nE "s/^[[:space:]]*\"${key}\":[[:space:]]*([0-9]+).*/\1/p" | head -n 1
}

json_string() {
  local key=$1
  sed -nE "s/^[[:space:]]*\"${key}\":[[:space:]]*\"([^\"]*)\".*/\1/p" | head -n 1
}

[[ -n "$DUMP_FILE" && -n "$MANIFEST_FILE" ]] \
  || die 'Использование: import-server-handoff.sh DUMP MANIFEST [--confirm-replace-database]'
DUMP_FILE=$(realpath "$DUMP_FILE")
MANIFEST_FILE=$(realpath "$MANIFEST_FILE")
test -s "$DUMP_FILE" || die 'Dump отсутствует или пуст.'
test -s "$MANIFEST_FILE" || die 'Manifest отсутствует или пуст.'
test -f "$DEPLOY_DIR/.env" || die 'Production .env отсутствует.'
test -f "$DEPLOY_DIR/secrets/vk_tokens.txt" || die 'Production token file отсутствует.'
command -v python3 >/dev/null || die 'Для безопасного чтения manifest нужен python3.'
command -v flock >/dev/null || die 'Для блокировки нужен flock.'
install -d -m 700 "$DEPLOY_DIR/.deploy"
exec 9>"$DEPLOY_DIR/.deploy/deploy.lock"
flock -n 9 || die 'Deployment или другой handoff уже выполняется.'

mapfile -t MANIFEST < <(python3 - "$MANIFEST_FILE" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8-sig"))
required = payload["classification"]
values = (
    payload["dump_file"],
    payload["sha256"],
    payload["database"],
    payload["run_id"],
    str(required["approved"]),
    str(required["rejected"]),
    str(required["pending"]),
    str(payload.get("collection_status", {}).get("jobs", {}).get("completed", 0)),
)
if any("\n" in str(value) or "\r" in str(value) for value in values):
    raise SystemExit("manifest contains newline")
print(*values, sep="\n")
PY
)
[[ ${#MANIFEST[@]} -eq 8 ]] || die 'Manifest неполон.'
MANIFEST_DUMP=${MANIFEST[0]}
EXPECTED_SHA=${MANIFEST[1]}
MANIFEST_DATABASE=${MANIFEST[2]}
RUN_ID=${MANIFEST[3]}
EXPECTED_APPROVED=${MANIFEST[4]}
EXPECTED_REJECTED=${MANIFEST[5]}
EXPECTED_PENDING=${MANIFEST[6]}
BASELINE_COMPLETED=${MANIFEST[7]}

[[ "$(basename "$DUMP_FILE")" == "$MANIFEST_DUMP" ]] || die 'dump_file в manifest не совпадает.'
[[ "$EXPECTED_SHA" =~ ^[0-9a-fA-F]{64}$ ]] || die 'Некорректный SHA256 в manifest.'
printf '%s  %s\n' "$EXPECTED_SHA" "$DUMP_FILE" | sha256sum --check --status \
  || die 'SHA256 dump не совпал с manifest.'

DATABASE=$(env_value POSTGRES_DB)
DATABASE_USER=$(env_value POSTGRES_USER)
[[ "$DATABASE" == "$MANIFEST_DATABASE" ]] || die 'Database в manifest не совпадает с production .env.'
[[ "$DATABASE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die 'Недопустимое имя database.'
[[ "$DATABASE_USER" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die 'Недопустимое имя PostgreSQL user.'

compose up -d postgres
compose exec -T postgres pg_isready -U "$DATABASE_USER" -d "$DATABASE" >/dev/null \
  || die 'PostgreSQL недоступен.'
compose exec -T postgres pg_restore --list < "$DUMP_FILE" >/dev/null \
  || die 'pg_restore --list отклонил dump.'

compose stop collector-worker || true
WORKER_CONTAINER=$(compose ps -q collector-worker 2>/dev/null || true)
if [[ -n "$WORKER_CONTAINER" ]]; then
  WORKER_STATE=$(docker inspect --format '{{.State.Status}}' "$WORKER_CONTAINER")
  [[ "$WORKER_STATE" != running ]] || die 'Server worker не остановлен.'
fi

USER_TABLES=$(compose exec -T postgres psql -U "$DATABASE_USER" -d "$DATABASE" -Atqc \
  "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
NONEMPTY=0
if [[ "$USER_TABLES" -gt 0 ]]; then
  NONEMPTY=$(compose exec -T postgres psql -U "$DATABASE_USER" -d "$DATABASE" -Atqc \
    "SELECT CASE WHEN EXISTS (SELECT 1 FROM collection_runs LIMIT 1) OR EXISTS (SELECT 1 FROM group_candidates LIMIT 1) THEN 1 ELSE 0 END" \
    2>/dev/null || printf '1\n')
fi
if [[ "$NONEMPTY" -eq 1 && "$CONFIRM" != --confirm-replace-database ]]; then
  die 'Серверная БД непуста; повторите с --confirm-replace-database.'
fi

install -d -m 700 "$DEPLOY_DIR/backups" "$DEPLOY_DIR/.deploy"
TIMESTAMP=$(date -u +%Y%m%d-%H%M%SZ)
if [[ "$NONEMPTY" -eq 1 ]]; then
  SERVER_BACKUP="$DEPLOY_DIR/backups/server-before-handoff-${TIMESTAMP}.dump"
  compose exec -T postgres pg_dump -U "$DATABASE_USER" -d "$DATABASE" -Fc > "$SERVER_BACKUP"
  test -s "$SERVER_BACKUP" || die 'Backup существующей серверной БД пуст.'
  compose exec -T postgres pg_restore --list < "$SERVER_BACKUP" >/dev/null \
    || die 'Backup существующей серверной БД не прошёл pg_restore --list.'
fi

compose exec -T postgres psql -U "$DATABASE_USER" -d postgres -v ON_ERROR_STOP=1 -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DATABASE' AND pid <> pg_backend_pid();" >/dev/null
compose exec -T postgres dropdb -U "$DATABASE_USER" --if-exists "$DATABASE"
compose exec -T postgres createdb -U "$DATABASE_USER" -O "$DATABASE_USER" "$DATABASE"
compose exec -T postgres pg_restore -U "$DATABASE_USER" -d "$DATABASE" \
  --exit-on-error --no-owner --no-privileges < "$DUMP_FILE"

compose run --rm collector alembic upgrade head
compose run --rm collector alembic check

CLASSIFICATION=$(compose run --rm collector classification summary)
APPROVED=$(printf '%s\n' "$CLASSIFICATION" | sed -nE 's/^Approved: ([0-9]+)$/\1/p')
REJECTED=$(printf '%s\n' "$CLASSIFICATION" | sed -nE 's/^Rejected: ([0-9]+)$/\1/p')
PENDING=$(printf '%s\n' "$CLASSIFICATION" | sed -nE 's/^Pending: ([0-9]+)$/\1/p')
[[ "$APPROVED" == "$EXPECTED_APPROVED" ]] || die 'Approved count не совпал с manifest.'
[[ "$REJECTED" == "$EXPECTED_REJECTED" ]] || die 'Rejected count не совпал с manifest.'
[[ "$PENDING" == "$EXPECTED_PENDING" ]] || die 'Pending count не совпал с manifest.'

STATUS_JSON=$(compose run --rm collector collection status --run-id "$RUN_ID")
printf '%s\n' "$STATUS_JSON"
RUN_STATUS=$(printf '%s\n' "$STATUS_JSON" | json_string status || true)
FAILED_JOBS=$(printf '%s\n' "$STATUS_JSON" | json_number failed || true)
[[ "$RUN_STATUS" != failed ]] || die 'Импортированный collection run имеет status=failed.'
[[ "${FAILED_JOBS:-0}" -eq 0 ]] || die 'В импортированном run есть failed jobs.'
set +e
VERIFY_JSON=$(compose run --rm collector collection verify --run-id "$RUN_ID" 2>&1)
VERIFY_CODE=$?
set -e
printf '%s\n' "$VERIFY_JSON"
for metric in post_duplicates membership_duplicates subscription_duplicates rejected_jobs; do
  value=$(printf '%s\n' "$VERIFY_JSON" | json_number "$metric" || true)
  [[ "${value:-1}" -eq 0 ]] || die "Проверка данных не пройдена: $metric=${value:-unknown}"
done
if [[ "$VERIFY_CODE" -ne 0 ]]; then
  printf '[handoff] Running jobs сохранены из локального run и будут возвращены из lease worker-ом.\n'
fi

compose up -d collector-worker
sleep "$PROGRESS_WAIT"
FINAL_STATUS=$(compose run --rm collector collection status --run-id "$RUN_ID")
FINAL_COMPLETED=$(printf '%s\n' "$FINAL_STATUS" | json_number completed || true)
FINAL_PENDING=$(printf '%s\n' "$FINAL_STATUS" | json_number pending || true)
FINAL_RUNNING=$(printf '%s\n' "$FINAL_STATUS" | json_number running || true)
FINAL_RETRY=$(printf '%s\n' "$FINAL_STATUS" | json_number retry_wait || true)
FINAL_COMPLETED=${FINAL_COMPLETED:-0}
FINAL_PENDING=${FINAL_PENDING:-0}
FINAL_RUNNING=${FINAL_RUNNING:-0}
FINAL_RETRY=${FINAL_RETRY:-0}
if (( FINAL_COMPLETED <= BASELINE_COMPLETED && FINAL_PENDING > 0 && FINAL_RUNNING == 0 && FINAL_RETRY == 0 )); then
  die 'Server worker не показал прогресс и не ожидает retry.'
fi

REPORT_FILE="$DEPLOY_DIR/.deploy/handoff-${TIMESTAMP}.report.txt"
{
  printf 'status=success\n'
  printf 'created_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'dump=%s\n' "$DUMP_FILE"
  printf 'sha256=%s\n' "$EXPECTED_SHA"
  printf 'database=%s\n' "$DATABASE"
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'classification_approved=%s\n' "$APPROVED"
  printf 'classification_rejected=%s\n' "$REJECTED"
  printf 'classification_pending=%s\n' "$PENDING"
  printf 'completed=%s\n' "$FINAL_COMPLETED"
} > "$REPORT_FILE"
printf '[handoff] Импорт завершён. Отчёт: %s\n' "$REPORT_FILE"
