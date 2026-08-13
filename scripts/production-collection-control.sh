#!/usr/bin/env bash
set -Eeuo pipefail

umask 077

ACTION=report
DEPLOY_DIR="${DEPLOY_ROOT:-/opt/vk-research-collector}"
EXPECTED_USER="${DEPLOY_USER:-deploy}"
EXPECTED_DEPLOY_DIR="${COLLECTION_EXPECTED_DEPLOY_DIR:-/opt/vk-research-collector}"
COLLECTOR_REPOSITORY="ghcr.io/marklyvk/vk-research-collector/collector"
WORKER_STOPPED=0

log() {
  printf '[collection-control] %s\n' "$*"
}

die() {
  printf '[collection-control] ОШИБКА: %s\n' "$*" >&2
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

collector() {
  compose run --rm --no-deps collector "$@"
}

psql_query() {
  compose exec -T postgres psql \
    -X -v ON_ERROR_STOP=1 -P pager=off \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
}

service_state() {
  local service=$1 container state health
  container=$(compose ps -q "$service" 2>/dev/null || true)
  [[ -n "$container" ]] || { printf 'absent\n'; return; }
  state=$(docker inspect --format '{{.State.Status}}' "$container")
  health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container")
  [[ -n "$health" ]] && printf '%s/%s\n' "$state" "$health" || printf '%s\n' "$state"
}

set_env_value() {
  local key=$1 value=$2 temporary
  temporary=$(mktemp "$DEPLOY_DIR/.env.tmp.XXXXXX")
  awk -v key="$key" -v value="$value" '
    BEGIN { found=0 }
    index($0, key "=") == 1 { print key "=" value; found=1; next }
    { print }
    END { if (!found) print key "=" value }
  ' "$DEPLOY_DIR/.env" > "$temporary"
  chmod --reference="$DEPLOY_DIR/.env" "$temporary"
  chown --reference="$DEPLOY_DIR/.env" "$temporary"
  mv -f -- "$temporary" "$DEPLOY_DIR/.env"
}

restart_worker() {
  if [[ "$WORKER_STOPPED" -eq 1 ]]; then
    log 'Возвращаю collector-worker в running state.'
    compose up -d --no-deps --no-build collector-worker
    WORKER_STOPPED=0
  fi
}

ensure_worker_healthy() {
  local attempt state
  state=$(service_state collector-worker)
  if [[ "$state" != running/healthy ]]; then
    log "Worker имеет status $state; выполняю безопасный self-heal."
    compose up -d --no-deps --no-build collector-worker
  fi
  for attempt in {1..30}; do
    state=$(service_state collector-worker)
    [[ "$state" == running/healthy ]] && return 0
    sleep 2
  done
  die "Worker не healthy после self-heal: $state"
}

grant_collector_backup_read() {
  local backup=$1 backup_dir
  backup_dir=$(dirname "$backup")
  chmod 0700 "$backup_dir"
  if command -v setfacl >/dev/null 2>&1; then
    setfacl -m u:10001:rx "$backup_dir"
    setfacl -m u:10001:r "$backup"
    log 'Worker получил rx ACL на закрытый каталог и read-only ACL на проверенный backup.'
  else
    chmod o+x "$backup_dir"
    chmod o+r "$backup"
    log 'setfacl отсутствует: каталогу добавлен только traverse, backup — только read.'
  fi
}

report() {
  local worker_state postgres_state current_commit
  postgres_state=$(service_state postgres)
  worker_state=$(service_state collector-worker)
  current_commit=$(docker image inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$CURRENT_IMAGE")

  log "UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  log "Image: $CURRENT_IMAGE"
  log "Application commit: ${current_commit:-unknown}"
  log "Alembic: $REVISION; PostgreSQL: $postgres_state; worker: $worker_state"
  log "Disk: $(df -hP "$DEPLOY_DIR" | awk 'NR==2 {print $3 " used, " $4 " available, " $5 " full"}')"
  log 'Безопасная runtime-конфигурация:'
  for key in \
    COLLECTION_POSTS_ENABLED COLLECTION_POSTS_MAX_PER_GROUP \
    COLLECTION_MEMBERS_ENABLED COLLECTION_MEMBERS_MAX_PER_GROUP \
    COLLECTION_USERS_ENABLED COLLECTION_SUBSCRIPTIONS_ENABLED \
    COLLECTION_SUBSCRIPTIONS_MAX_PER_USER COLLECTION_SUBSCRIPTIONS_PAGE_SIZE \
    COLLECTION_SUBSCRIPTIONS_USERS_PER_RUN COLLECTION_SUBSCRIPTION_PILOT_USERS \
    COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED COLLECTION_SUBSCRIPTION_GROUP_POSTS_MAX; do
    printf '%s=%s\n' "$key" "$(env_value "$key")"
  done

  psql_query <<'SQL'
\echo '=== ПОИСК И КЛАССИФИКАЦИЯ СООБЩЕСТВ ==='
SELECT 'group_candidates' AS element, count(*) AS total FROM group_candidates
UNION ALL SELECT 'search_keywords', count(*) FROM search_keywords
UNION ALL SELECT 'group_keyword_matches', count(*) FROM group_keyword_matches
UNION ALL SELECT 'search_runs', count(*) FROM search_runs
UNION ALL SELECT 'classification_reviews', count(*) FROM classification_reviews
UNION ALL SELECT 'group_labels', count(*) FROM group_labels
ORDER BY element;

SELECT classification_status::text AS classification_status, count(*) AS groups
FROM group_candidates GROUP BY 1 ORDER BY 1;

SELECT k.subject,
       count(DISTINCT m.group_id) AS distinct_groups,
       count(*) AS keyword_links,
       count(DISTINCT m.keyword_id) AS matched_keywords
FROM group_keyword_matches m
JOIN search_keywords k ON k.id = m.keyword_id
GROUP BY k.subject ORDER BY k.subject;

SELECT label, count(*) AS groups FROM group_labels GROUP BY label ORDER BY groups DESC, label;
SELECT status::text AS status, count(*) AS runs, sum(api_results_count) AS api_results,
       sum(error_count) AS errors
FROM search_runs GROUP BY 1 ORDER BY 1;

\echo '=== СОБРАННЫЙ КОНТЕНТ И СВЯЗИ ==='
SELECT 'vk_communities' AS element, count(*) AS total FROM vk_communities
UNION ALL SELECT 'communities_with_metadata', count(*) FROM vk_communities WHERE metadata_updated_at IS NOT NULL
UNION ALL SELECT 'group_posts', count(*) FROM group_posts
UNION ALL SELECT 'post_attachments', count(*) FROM post_attachments
UNION ALL SELECT 'vk_users', count(*) FROM vk_users
UNION ALL SELECT 'accessible_vk_users', count(*) FROM vk_users WHERE deactivated IS NULL AND (NOT is_closed OR can_access_closed)
UNION ALL SELECT 'group_memberships', count(*) FROM group_memberships
UNION ALL SELECT 'current_group_memberships', count(*) FROM group_memberships WHERE is_current
UNION ALL SELECT 'user_group_subscriptions', count(*) FROM user_group_subscriptions
UNION ALL SELECT 'current_user_group_subscriptions', count(*) FROM user_group_subscriptions WHERE is_current
UNION ALL SELECT 'processed_subscription_users', count(*) FROM user_subscription_states WHERE last_success_at IS NOT NULL
UNION ALL SELECT 'private_subscription_users', count(*) FROM user_subscription_states WHERE privacy_denied
UNION ALL SELECT 'subscription_post_states', count(*) FROM community_post_collection_states
UNION ALL SELECT 'subscription_communities_with_posts', count(*) FROM community_post_collection_states WHERE last_success_at IS NOT NULL
ORDER BY element;

\echo '=== ЗАПУСКИ И ОЧЕРЕДЬ ==='
SELECT scope, status::text AS status, count(*) AS runs,
       sum(total_jobs) AS jobs, sum(completed_jobs) AS completed,
       sum(failed_jobs) AS failed, sum(skipped_jobs) AS skipped
FROM collection_runs GROUP BY scope, status ORDER BY scope, status;

SELECT job_type, status::text AS status, count(*) AS jobs,
       sum(api_requests) AS api_requests, sum(rows_inserted) AS rows_inserted,
       sum(rows_updated) AS rows_updated
FROM collection_jobs GROUP BY job_type, status ORDER BY job_type, status;

SELECT id, scope, status::text AS status, created_at, started_at, finished_at,
       total_jobs, completed_jobs, failed_jobs, skipped_jobs, next_wakeup_at,
       left(coalesce(error_message, ''), 180) AS error
FROM collection_runs ORDER BY created_at DESC LIMIT 10;

\echo '=== ОШИБКИ И METHOD-AWARE COOLDOWN ==='
SELECT endpoint, error_category, coalesce(vk_error_code, 0) AS vk_error_code, count(*) AS errors
FROM collection_job_errors GROUP BY endpoint, error_category, vk_error_code
ORDER BY errors DESC, endpoint LIMIT 30;

SELECT DISTINCT ON (endpoint, error_category, coalesce(vk_error_code, 0))
       endpoint, error_category, coalesce(vk_error_code, 0) AS vk_error_code,
       created_at AS latest_at, left(sanitized_message, 180) AS latest_message
FROM collection_job_errors
ORDER BY endpoint, error_category, coalesce(vk_error_code, 0), created_at DESC;

SELECT r.scope, j.job_type, j.entity_type, j.entity_id, j.attempt_count,
       left(coalesce(j.last_error_type, ''), 80) AS error_type,
       left(coalesce(j.last_error_message, ''), 180) AS error_message
FROM collection_jobs j
JOIN collection_runs r ON r.id = j.collection_run_id
WHERE j.status = 'failed'
ORDER BY j.finished_at DESC NULLS LAST
LIMIT 30;

SELECT method, count(*) AS token_states,
       count(*) FILTER (WHERE blocked_until > now()) AS currently_blocked,
       min(blocked_until) FILTER (WHERE blocked_until > now()) AS nearest_unblock,
       max(blocked_until) FILTER (WHERE blocked_until > now()) AS latest_unblock
FROM vk_token_method_states GROUP BY method ORDER BY method;

\echo '=== РАЗМЕР БАЗЫ И КРУПНЕЙШИЕ ТАБЛИЦЫ ==='
SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;
SELECT relname AS relation, pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
       n_live_tup AS estimated_rows
FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 15;
SQL
}

start_subscriptions() {
  local active_runs gate_applied latest_backup pilot_attempt pilot_state plan_state report_path
  local deferred_pilot production_allowed plan_output retryable_pilot run_id
  active_runs=$(psql_query -Atqc \
    "SELECT count(*)
       FROM collection_runs
      WHERE scope IN ('full','incremental','subscriptions','subscription_posts')
        AND status::text IN ('planned','running','waiting_method_limit')")
  if [[ "$active_runs" != 0 ]]; then
    log "Активных collection runs: $active_runs. Следующая cohort пока не нужна."
    return
  fi

  latest_backup=$(find "$DEPLOY_DIR/backups" -type f -name '*.dump' -printf '%T@ %p\n' \
    | sort -nr | head -n 1 | cut -d' ' -f2-)
  [[ -n "$latest_backup" && -s "$latest_backup" ]] || die 'Нет непустого backup для Gate A.'
  compose exec -T postgres pg_restore --list < "$latest_backup" >/dev/null \
    || die 'Последний backup не прошёл pg_restore --list.'
  grant_collector_backup_read "$latest_backup"
  collector python -c \
    'from pathlib import Path; p=Path("/app/backups/'"$(basename "$latest_backup")"'"); f=p.open("rb"); assert f.read(5) == b"PGDMP"' \
    || die 'Collector UID не может прочитать PGDMP header после настройки ACL.'

  log 'Фиксирую безопасный лимит подписок: 50 на пользователя и включаю phase A.'
  set_env_value COLLECTION_SUBSCRIPTIONS_ENABLED true
  set_env_value COLLECTION_SUBSCRIPTIONS_MAX_PER_USER 50
  set_env_value COLLECTION_SUBSCRIPTIONS_PAGE_SIZE 50

  log 'Останавливаю worker на время измеряемого Pilot A.'
  compose stop -t 360 collector-worker
  WORKER_STOPPED=1
  trap restart_worker EXIT

  report_path="$DEPLOY_DIR/exports/stage2-pilot/subscription-gate-a.json"
  gate_applied=0
  run_id=$(psql_query -Atqc \
    "SELECT id
       FROM collection_runs
      WHERE scope='subscriptions'
        AND status::text='paused_capacity_limit'
        AND created_at > coalesce(
          (SELECT max(finished_at)
             FROM collection_runs
            WHERE scope='subscriptions'
              AND status::text IN ('completed','completed_with_errors')),
          '-infinity'::timestamptz
        )
      ORDER BY created_at DESC
      LIMIT 1")
  if [[ -n "$run_id" && -s "$report_path" ]]; then
    log "Пробую применить уже измеренный Gate A к незапущенному run $run_id."
    if collector collection capacity-apply \
      --run-id "$run_id" \
      --source /app/exports/stage2-pilot/subscription-gate-a.json \
      --backup "/app/backups/$(basename "$latest_backup")"; then
      gate_applied=1
    else
      log 'Существующий report не подошёл; выполняю новый измеряемый Pilot A.'
    fi
  fi

  if [[ "$gate_applied" -eq 0 ]]; then
    for pilot_attempt in 1 2 3; do
      log "Запускаю Pilot A подписок, попытка $pilot_attempt/3 (capacity gate не обходится)."
      collector collection subscriptions pilot
      [[ -s "$report_path" ]] || die "Pilot A не создал отчёт: $report_path"
      pilot_state=$(python3 -c \
        'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8")); m=p["measured"]; allowed=p["production_allowed"] is True; retry=(not allowed and m["planned_entities"] > 0 and m["completed_entities"] == 0 and m["failed_entities"] == 0 and m["skipped_entities"] == m["planned_entities"]); deferred=(not allowed and m.get("deferred_entities", 0) > 0); print(f"{str(allowed).lower()}|{str(retry).lower()}|{str(deferred).lower()}")' \
        "$report_path")
      IFS='|' read -r production_allowed retryable_pilot deferred_pilot <<< "$pilot_state"
      [[ "$production_allowed" == true ]] && break
      if [[ "$deferred_pilot" == true ]]; then
        log 'Pilot сохранил transient retry в PostgreSQL; продолжение выполнит следующий hourly-control.'
        restart_worker
        trap - EXIT
        return
      fi
      if [[ "$retryable_pilot" == true && "$pilot_attempt" -lt 3 ]]; then
        log 'Pilot целиком пропущен из-за terminal-состояний; выбираю следующую cohort.'
        continue
      fi
      die 'Pilot A завершён, но capacity report не разрешает production run.'
    done

    log 'Создаю production cohort подписок.'
    plan_output=$(collector collection subscriptions plan)
    printf '%s\n' "$plan_output"
    run_id=$(grep -Eo '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' \
      <<< "$plan_output" | tail -n 1)
    [[ -n "$run_id" ]] || die 'Не удалось получить run ID из production plan.'
    plan_state=$(psql_query -AtF '|' -c \
      "SELECT status::text, total_jobs FROM collection_runs WHERE id='$run_id'::uuid")
    if [[ "$plan_state" == 'completed|0' ]]; then
      log 'Подходящих пользователей для новой cohort сейчас нет.'
      restart_worker
      trap - EXIT
      return
    fi
    [[ "$plan_state" == "paused_capacity_limit|"* ]] \
      || die "Production plan имеет неожиданный status/jobs: $plan_state"

    log "Применяю проверенный Gate A к run $run_id."
    collector collection capacity-apply \
      --run-id "$run_id" \
      --source /app/exports/stage2-pilot/subscription-gate-a.json \
      --backup "/app/backups/$(basename "$latest_backup")"
  fi

  restart_worker
  trap - EXIT
  sleep 20
  collector collection subscriptions status --run-id "$run_id"
  printf 'STARTED_SUBSCRIPTIONS_RUN_ID=%s\n' "$run_id"
  printf 'CAPACITY_REPORT=%s\n' "$report_path"
  printf 'VERIFIED_BACKUP=%s\n' "$latest_backup"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --action) ACTION=${2:?}; shift 2 ;;
    --deploy-dir) DEPLOY_DIR=${2:?}; shift 2 ;;
    -h|--help) printf 'usage: %s --action report|start-subscriptions [--deploy-dir PATH]\n' "$0"; exit 0 ;;
    *) die "Неизвестный аргумент: $1" ;;
  esac
done

[[ "$ACTION" == report || "$ACTION" == start-subscriptions ]] \
  || die "Неизвестное действие: $ACTION"
for command in docker flock realpath sed awk find sort df grep mktemp python3 chmod chown; do
  command -v "$command" >/dev/null 2>&1 || die "Не найдена команда: $command"
done
docker compose version >/dev/null
[[ "$(id -un)" == "$EXPECTED_USER" ]] || die "Скрипт должен запускаться пользователем $EXPECTED_USER."
DEPLOY_DIR=$(realpath -e "$DEPLOY_DIR")
EXPECTED_DEPLOY_DIR=$(realpath -e "$EXPECTED_DEPLOY_DIR")
[[ "$DEPLOY_DIR" == "$EXPECTED_DEPLOY_DIR" ]] || die "Разрешён только $EXPECTED_DEPLOY_DIR."
test -f "$DEPLOY_DIR/.env" || die 'Не найден production .env.'
test -f "$DEPLOY_DIR/.deploy/current-image" || die 'Не найден current-image.'

exec 9>"$DEPLOY_DIR/.deploy/deploy.lock"
flock -n 9 || die 'Другой production deployment/maintenance уже выполняется.'
CURRENT_IMAGE=$(tr -d '\r\n' < "$DEPLOY_DIR/.deploy/current-image")
[[ "$CURRENT_IMAGE" == "$COLLECTOR_REPOSITORY:sha-"* ]] || die 'Некорректный immutable image.'
docker image inspect "$CURRENT_IMAGE" >/dev/null
compose config --quiet
POSTGRES_USER=$(env_value POSTGRES_USER); POSTGRES_USER=${POSTGRES_USER:-vk_collector}
POSTGRES_DB=$(env_value POSTGRES_DB); POSTGRES_DB=${POSTGRES_DB:-vk_research}
[[ "$(service_state postgres)" == running/healthy ]] || die 'PostgreSQL не healthy.'
ensure_worker_healthy
REVISION=$(psql_query -Atqc 'SELECT version_num FROM alembic_version')
[[ "$REVISION" == 20260810_0007 ]] || die "Неожиданная Alembic revision: $REVISION"

report
if [[ "$ACTION" == start-subscriptions ]]; then
  start_subscriptions
  log 'Срез после запуска:'
  report
fi
