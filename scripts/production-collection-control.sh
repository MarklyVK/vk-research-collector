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

\echo '=== COVERAGE ПОДПИСОК И ЛИЧНЫХ СТЕН ==='
WITH eligible AS (
  SELECT DISTINCT u.vk_id
  FROM vk_users u
  JOIN group_memberships m ON m.user_id=u.vk_id AND m.is_current
  JOIN group_candidates g ON g.id=m.group_id AND g.classification_status='approved'
  WHERE u.deactivated IS NULL AND (NOT u.is_closed OR u.can_access_closed)
), subscription_coverage AS (
  SELECT count(*) AS eligible,
         count(*) FILTER (WHERE s.next_scheduled_at > now()
           AND (s.last_success_at IS NOT NULL OR s.terminal_reason IS NOT NULL)) AS fresh,
         count(*) FILTER (WHERE s.next_scheduled_at > now()
           AND s.terminal_reason IS NOT NULL) AS terminal
  FROM eligible e LEFT JOIN user_subscription_states s ON s.user_id=e.vk_id
), wall_coverage AS (
  SELECT count(*) AS eligible,
         count(*) FILTER (WHERE s.next_scheduled_at > now()
           AND (s.last_success_at IS NOT NULL OR s.wall_private OR s.unavailable)) AS fresh,
         count(*) FILTER (WHERE s.next_scheduled_at > now()
           AND (s.wall_private OR s.unavailable)) AS terminal
  FROM eligible e LEFT JOIN user_post_collection_states s ON s.user_id=e.vk_id
)
SELECT 'subscriptions' AS scope, eligible, eligible-fresh AS due, fresh, terminal
FROM subscription_coverage
UNION ALL
SELECT 'user_posts', eligible, eligible-fresh, fresh, terminal FROM wall_coverage;

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
UNION ALL SELECT 'user_posts', count(*) FROM user_posts
UNION ALL SELECT 'user_post_attachments', count(*) FROM user_post_attachments
UNION ALL SELECT 'user_post_states', count(*) FROM user_post_collection_states
UNION ALL SELECT 'processed_user_posts', count(*) FROM user_post_collection_states WHERE last_success_at IS NOT NULL
UNION ALL SELECT 'private_user_walls', count(*) FROM user_post_collection_states WHERE wall_private
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

\echo '=== КАМПАНИИ И КАНОНИЧЕСКИЙ BACKLOG ==='
SELECT id, campaign_type, status, phase, snapshot_at, started_at, finished_at,
       next_wakeup_at, left(coalesce(error_message, ''), 180) AS error
FROM collection_campaigns ORDER BY created_at DESC LIMIT 10;

SELECT job_type, status::text AS status, count(*) AS job_rows,
       count(DISTINCT (entity_type, entity_id)) AS distinct_entities
FROM collection_jobs GROUP BY job_type, status ORDER BY job_type, status;

SELECT count(*) FILTER (WHERE status='running' AND locked_at < now() - interval '5 minutes')
         AS stale_running_leases,
       count(*) FILTER (WHERE status IN ('pending','retry_wait')) AS queued_rows
FROM collection_jobs;

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
  local active_campaigns active_runs paused_capacity_campaigns
  local campaign_action campaign_decision_json campaign_id gate_applied latest_backup
  local pilot_attempt pilot_state plan_state report_path
  local deferred_pilot production_allowed plan_output renew_run_id retryable_pilot run_id run_phase
  local pilot_decision pilot_decision_json pilot_ids pilot_run_id pilot_scope pilot_wakeup
  log 'Фиксирую безопасный лимит подписок: 50 на пользователя и включаю phase A.'
  set_env_value COLLECTION_SUBSCRIPTIONS_ENABLED true
  set_env_value COLLECTION_SUBSCRIPTIONS_MAX_PER_USER 50
  set_env_value COLLECTION_SUBSCRIPTIONS_PAGE_SIZE 50
  active_campaigns=$(psql_query -Atqc \
    "SELECT count(*) FROM collection_campaigns
      WHERE campaign_type='subscription_enrichment'
        AND status IN ('planned','running','paused','waiting_method_limit','paused_capacity_limit')")
  active_runs=$(psql_query -Atqc \
    "SELECT count(*)
       FROM collection_runs
      WHERE scope IN ('subscriptions','subscription_posts',
                      'subscription_discovery','subscription_metadata')
        AND status::text IN ('planned','running','paused','paused_no_tokens',
                             'waiting_method_limit')")
  paused_capacity_campaigns=$(psql_query -Atqc \
    "SELECT count(*) FROM collection_campaigns
      WHERE campaign_type='subscription_enrichment'
        AND status='paused_capacity_limit'")
  pilot_decision_json=$(collector collection subscriptions pilot-control-decision)
  pilot_state=$(python3 -c \
    'import json,sys; p=json.load(sys.stdin); print("|".join(str(p.get(k) or "") for k in ("action","run_id","scope","next_wakeup_at"))); print(",".join(p.get("pilot_ids", [])))' \
    <<< "$pilot_decision_json")
  IFS='|' read -r pilot_decision pilot_run_id pilot_scope pilot_wakeup <<< "$(head -n 1 <<< "$pilot_state")"
  pilot_ids=$(tail -n 1 <<< "$pilot_state")
  if [[ "$pilot_decision" == wait ]]; then
    [[ "$pilot_scope" == subscriptions_pilot ]] \
      || die "Незавершённый $pilot_scope требует явного операторского решения."
    log "Pilot $pilot_run_id ожидает persisted retry до $pilot_wakeup; новый pilot не создаётся."
    return
  fi
  if [[ "$pilot_decision" == operator_required ]]; then
    die "Неоднозначные/несовместимые pilot: $pilot_ids. Выполните: collector collection subscriptions pilot-preview; затем cancel-pilot --run-id ID --confirm."
  fi
  if [[ "$pilot_decision" == resume ]]; then
    [[ "$pilot_scope" == subscriptions_pilot ]] \
      || die "Hourly-control не возобновляет $pilot_scope автоматически; используйте pilot-preview."
    log "Совместимый existing Pilot A будет возобновлён по run ID $pilot_run_id."
  fi
  campaign_decision_json=$(collector collection campaign control-decision)
  plan_state=$(python3 -c \
    'import json,sys; p=json.load(sys.stdin); print("|".join(str(p.get(k) or "") for k in ("action","run_id","scope")))' \
    <<< "$campaign_decision_json")
  IFS='|' read -r campaign_action renew_run_id run_phase <<< "$plan_state"
  if [[ "$campaign_action" == operator_required || "$campaign_action" == operator_paused ]]; then
    die "Campaign требует операторского решения: $campaign_decision_json"
  fi
  if [[ "$campaign_action" == reuse_active ]]; then
    log "Активная campaign уже имеет runnable/waiting work: $campaign_decision_json"
    collector collection campaign status
    return
  fi
  if [[ "$active_runs" != 0 ]]; then
    log "Активные кампании=$active_campaigns, runs=$active_runs. Дубли не создаются."
    collector collection campaign status
    collector collection backlog
    return
  fi
  if [[ "$active_campaigns" != 0 && "$paused_capacity_campaigns" == 0 ]]; then
    log "Активная campaign переиспользуется worker; новый pilot/run не создаётся."
    collector collection campaign status
    collector collection backlog
    return
  fi
  if [[ "$paused_capacity_campaigns" != 0 ]]; then
    log "Переиспользую paused-capacity campaign; проверяю путь Gate A apply/renew."
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

  log 'Останавливаю worker на время измеряемого Pilot A.'
  compose stop -t 360 collector-worker
  WORKER_STOPPED=1
  trap restart_worker EXIT

  report_path="$DEPLOY_DIR/exports/stage2-pilot/subscription-gate-a.json"
  gate_applied=0
  run_id=$renew_run_id
  if [[ -n "$run_id" && -s "$report_path" ]]; then
    log "Пробую renewal Gate A для существующего $run_phase run $run_id; новый discovery не создаётся."
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
      if [[ "$pilot_decision" == resume ]]; then
        collector collection subscriptions pilot --run-id "$pilot_run_id"
        pilot_decision=continued
      else
        collector collection subscriptions pilot
      fi
      [[ -s "$report_path" ]] || die "Pilot A не создал отчёт: $report_path"
      pilot_state=$(python3 -c \
        'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8")); m=p["measured"]; allowed=p["production_allowed"] is True; retry=(not allowed and m["planned_entities"] > 0 and m["completed_entities"] == 0 and m["failed_entities"] == 0 and m["skipped_entities"] == m["planned_entities"]); deferred=(not allowed and m.get("deferred_entities", 0) > 0); print(f"{str(allowed).lower()}|{str(retry).lower()}|{str(deferred).lower()}")' \
        "$report_path")
      IFS='|' read -r production_allowed retryable_pilot deferred_pilot <<< "$pilot_state"
      [[ "$production_allowed" == true ]] && break
      if [[ "$deferred_pilot" == true ]]; then
        pilot_run_id=$(python3 -c \
          'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["run_id"])' \
          "$report_path")
        log "Pilot набрал репрезентативный минимум; завершаю deferred jobs без удаления данных, run ID $pilot_run_id."
        collector collection subscriptions finalize-deferred-pilot \
          --run-id "$pilot_run_id" \
          --confirmation FINALIZE_DEFERRED_SUBSCRIPTION_PILOT
        collector collection subscriptions finalized-pilot-report \
          --run-id "$pilot_run_id" \
          --confirmation REPORT_FINALIZED_SUBSCRIPTION_PILOT
        pilot_state=$(python3 -c \
          'import json,sys; p=json.load(open(sys.argv[1], encoding="utf-8")); print(str(p["production_allowed"] is True).lower())' \
          "$report_path")
        [[ "$pilot_state" == true ]] \
          || die 'Завершённый Pilot A не прошёл собственный capacity gate.'
        break
      fi
      if [[ "$retryable_pilot" == true && "$pilot_attempt" -lt 3 ]]; then
        log 'Pilot целиком пропущен из-за terminal-состояний; выбираю следующую cohort.'
        continue
      fi
      die 'Pilot A завершён, но capacity report не разрешает production run.'
    done

    if [[ -n "$renew_run_id" ]]; then
      log "Применяю свежий Gate A и metadata evidence к existing $run_phase run $renew_run_id."
      collector collection capacity-apply \
        --run-id "$renew_run_id" \
        --source /app/exports/stage2-pilot/subscription-gate-a.json \
        --backup "/app/backups/$(basename "$latest_backup")"
      run_id=$renew_run_id
    else
      log 'Создаю или переиспользую кампанию и первый discovery cohort.'
    plan_output=$(collector collection campaign plan --apply \
      --source /app/exports/stage2-pilot/subscription-gate-a.json \
      --backup "/app/backups/$(basename "$latest_backup")")
    printf '%s\n' "$plan_output"
    run_id=$(grep -Eo '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' \
      <<< "$plan_output" | tail -n 1)
    [[ -n "$run_id" ]] || die 'Не удалось получить run ID из production plan.'
    campaign_id=$run_id
    run_id=$(psql_query -Atqc \
      "SELECT r.id FROM collection_runs r
        JOIN collection_campaigns c ON c.id=r.campaign_id
       WHERE c.id='$campaign_id'::uuid
         AND r.scope IN ('subscription_discovery','subscription_metadata')
       ORDER BY r.created_at DESC LIMIT 1")
    if [[ -z "$run_id" ]]; then
      plan_state=$(psql_query -Atqc \
        "SELECT status FROM collection_campaigns WHERE id='$campaign_id'::uuid")
      if [[ "$plan_state" == completed ]]; then
        log 'Snapshot не содержит due discovery/metadata work; campaign завершена как no-op.'
        restart_worker
        trap - EXIT
        return
      fi
      die 'Кампания не создала runnable discovery/metadata cohort.'
    fi
    plan_state=$(psql_query -AtF '|' -c \
      "SELECT status::text, total_jobs FROM collection_runs WHERE id='$run_id'::uuid")
    if [[ "$plan_state" == 'completed|0' ]]; then
      log 'Подходящих пользователей для новой cohort сейчас нет.'
      restart_worker
      trap - EXIT
      return
    fi
      [[ "$plan_state" == "planned|"* || "$plan_state" == "completed|0" ]] \
        || die "Production plan имеет неожиданный status/jobs: $plan_state"
    fi
  fi

  restart_worker
  trap - EXIT
  sleep 20
  collector collection campaign status
  printf 'STARTED_SUBSCRIPTIONS_RUN_ID=%s\n' "$run_id"
  printf 'CAPACITY_REPORT=%s\n' "$report_path"
  printf 'VERIFIED_BACKUP=%s\n' "$latest_backup"
}

quarantine_incompatible_pilots() {
  collector collection legacy-pilots-preview
  collector collection quarantine-incompatible-pilots \
    --confirmation QUARANTINE_INCOMPATIBLE_PILOTS
}

start_user_posts() {
  local latest_backup report_path
  log 'Фиксирую owner-authorized лимиты личных стен: 20 постов, окно 180 дней.'
  set_env_value COLLECTION_USER_POSTS_ENABLED true
  set_env_value COLLECTION_USER_POSTS_MAX_PER_USER 20
  set_env_value COLLECTION_USER_POSTS_PAGE_SIZE 20
  set_env_value COLLECTION_USER_POSTS_WINDOW_DAYS 180
  latest_backup=$(find "$DEPLOY_DIR/backups" -type f -name '*.dump' -printf '%T@ %p\n' \
    | sort -nr | head -n 1 | cut -d' ' -f2-)
  [[ -n "$latest_backup" && -s "$latest_backup" ]] || die 'Нет непустого backup для user-post gate.'
  compose exec -T postgres pg_restore --list < "$latest_backup" >/dev/null \
    || die 'Последний backup не прошёл pg_restore --list.'
  grant_collector_backup_read "$latest_backup"
  log 'Останавливаю worker на время измеряемого user-post pilot.'
  compose stop -t 360 collector-worker
  WORKER_STOPPED=1
  trap restart_worker EXIT
  collector collection user-posts pilot
  report_path="$DEPLOY_DIR/exports/stage2-pilot/user-posts-pilot.json"
  [[ -s "$report_path" ]] || die "User-post pilot не создал отчёт: $report_path"
  collector collection user-posts capacity-apply \
    --source /app/exports/stage2-pilot/user-posts-pilot.json \
    --backup "/app/backups/$(basename "$latest_backup")"
  restart_worker
  trap - EXIT
  collector collection user-posts status
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --action) ACTION=${2:?}; shift 2 ;;
    --deploy-dir) DEPLOY_DIR=${2:?}; shift 2 ;;
    -h|--help) printf 'usage: %s --action report|start-subscriptions|start-user-posts|quarantine-incompatible-pilots [--deploy-dir PATH]\n' "$0"; exit 0 ;;
    *) die "Неизвестный аргумент: $1" ;;
  esac
done

[[ "$ACTION" == report || "$ACTION" == start-subscriptions || "$ACTION" == start-user-posts || "$ACTION" == quarantine-incompatible-pilots ]] \
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
[[ "$REVISION" == 20260820_0013 ]] || die "Неожиданная Alembic revision: $REVISION"

report
if [[ "$ACTION" == start-subscriptions ]]; then
  start_subscriptions
  log 'Срез после запуска:'
  report
fi
if [[ "$ACTION" == start-user-posts ]]; then
  start_user_posts
  log 'Срез после запуска личных стен:'
  report
fi
if [[ "$ACTION" == quarantine-incompatible-pilots ]]; then
  quarantine_incompatible_pilots
  log 'Срез после карантина legacy pilots:'
  report
fi
