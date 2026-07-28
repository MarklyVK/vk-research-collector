# Итоговый отчёт второго этапа

Дата: 28.07.2026. Ветка: `feat/approved-data-collection`.

## Статус

Техническая реализация, миграции, fake/integration-тесты, Docker и реальный pilot
завершены. Этап не объявляется полностью завершённым: измеренный прогноз 13,54 GiB
превышает безопасный лимит 7 GiB, поэтому основной run спланирован, но поставлен в
`paused_capacity_limit`. Remote GitHub Actions не запускался, поскольку push запрещён.

## Реализация

Спроектированы PostgreSQL queue, lease/heartbeat, `FOR UPDATE SKIP LOCKED`, page-level
transactions/checkpoints, retry/cooldown, idempotent upsert, snapshot semantics,
capacity/disk gates, privacy-команды, best-effort Telegram и SIGTERM shutdown.

Добавлены таблицы `collection_runs`, расширенная `collection_jobs`,
`group_collection_states`, `group_posts`, `post_attachments`, `vk_users`,
`group_memberships`, `user_group_subscriptions`, `collection_errors`. Миграции:
`20260728_0002_stage2_collection.py` и `20260728_0003_vk_user_natural_key.py`.
Цепочка 0001→0002→0003 проверена на текущей и отдельной чистой БД; repeat upgrade и
`alembic check` проходят.

Реализованы scopes: refresh групп, posts, attachment metadata, public members, batch
public profiles и public group subscriptions. Subscriptions реализованы и fake-tested,
но выключены и не запускались в реальном pilot. Сохраняемые профили минимальны; contacts,
binary media и raw VK JSON отсутствуют.

CLI: `collection plan/run/status/pause/resume/retry-failed/verify/summary/pilot`,
`collection capacity-apply`, совместимый `collection start`, а также
`privacy inspect-user/delete-user/inspect-group`. Full worker отказывается работать без
измеренного `capacity_gate=passed`.

## Очистка fixtures

По сочетанию точного test screen name, UUID keyword и отдельного batch подтверждены и
транзакционно удалены 12 integration fixtures: 6 approved и 6 rejected, отсутствовавшие
среди 37 407 ID в `exports/classification-final`. Удалены только шесть test batch и
связанные test keywords/runs. Итог: approved=12 260, rejected=25 147, pending=0,
orphans=0. Integration tests переведены на внешнюю savepoint/rollback transaction.

## Pilot и capacity

- Pilot run: `4c539596-288a-4141-a08a-f3e6887ad1b0`, status `completed`.
- Seed 20260728, 35 групп: 14 food_delivery, 15 customer_acquisition,
  11 tender_support, 5 multi-label.
- Duration: 138,12 s; реальный forced interruption продолжен с тем же run/checkpoints.
- VK requests: 131; retries: 0; failed: 0; skipped: 5.
- Skip-причина: VK error 15 `Access denied: group hide members` (5 раз).
- Группы refreshed: 35; posts: 3 969; attachments: 7 261;
  memberships: 12 316; users: 12 285; subscriptions: 0.
- Дубли posts/memberships/subscriptions: 0; rejected jobs: 0; running locks: 0.
- БД до/после pilot: 85 777 431 / 117 513 239 байт; прирост 31 735 808.
- Прогноз с резервом 30%: 14 537 357 656 байт (13,54 GiB).
- Capacity gate: `failed`; safe limit: 7 516 192 768 байт (7 GiB).

Основной run `301fe7a5-be50-4b31-9640-147e067c4045` имеет 36 780 pending jobs и status
`paused_capacity_limit`; API requests=0, то есть основной сбор не запускался. Повторный
plan переиспользует этот run. Рекомендуемый новый pilot: posts=100, members=200,
subscriptions=false. Это лишь кандидат безопасного режима, пока новый pilot не измерен.

## Проверки

- `ruff check .`: passed.
- `ruff format --check .`: passed.
- `mypy src`: passed.
- Local pytest: 18 passed, 2 PostgreSQL tests skipped вне integration environment.
- Docker/PostgreSQL pytest: 20 passed.
- `docker compose config`, build, postgres health и migration head: passed.
- Clean database migration и `alembic check`: passed.
- Fake full path, rerun dedupe, lease recovery, snapshots, privacy rollback и Telegram
  failure isolation: passed.
- Реальный pilot verify: passed.
- Secret scan: токены не найдены в tracked Git; значения runtime tokens не выводились.
- Remote CI: не запускался (push не выполнялся).

Ключевые EXPLAIN используют queue lease index, unique owner/post index,
membership-user index и profile TTL index. Локальная bind-файловая система показывала
84,4% при последнем plan, то есть близка к warning=85%; новые тяжёлые jobs сверх уже
приостановленного плана создавать не следует.

## Backup

- `backups/before-classification-import-20260727-232746Z.dump` — 20 915 232 байта.
- `backups/stage2-test-cleanup-20260728-000455Z.dump` — 21 519 079 байт.
- `backups/stage2-pilot-20260728-002754Z.dump` — 21 551 090 байт.

Все файлы ненулевые и прочитаны `pg_restore --list`; каталог исключён из Git.

## Коммиты

- `be808e3` checkpoint stage 1;
- `dfc26ea` проектирование;
- `6a2398b` схема stage 2;
- `24acc35` queue и workers;
- `ac1dcd1` CLI и privacy;
- `d79c960` integration coverage;
- `dbe5e43` runtime/CI operations;
- `9bcd0a3` verified pilot;
- `dc544aa` planning/recovery hardening;
- `becd4f1` measured capacity gate;
- `e927ed6` Telegram failure isolation.

## Продолжение

Новый уменьшенный pilot:

```bash
make backup PURPOSE=reduced-repilot
docker compose run --rm \
  -e COLLECTION_POSTS_MAX_PER_GROUP=100 \
  -e COLLECTION_MEMBERS_MAX_PER_GROUP=200 \
  -e COLLECTION_SUBSCRIPTIONS_ENABLED=false \
  collector collection pilot
```

Если новый `exports/stage2-pilot/capacity-estimate.json` содержит `decision=passed`:

```bash
make collection-capacity-apply RUN_ID=301fe7a5-be50-4b31-9640-147e067c4045
make collection-run RUN_ID=301fe7a5-be50-4b31-9640-147e067c4045
make collection-verify RUN_ID=301fe7a5-be50-4b31-9640-147e067c4045
```

Если report снова failed, `capacity-apply` завершится отказом и run останется на паузе.
Основные документы: `docs/STAGE2_REQUIREMENTS.md`, `docs/STAGE2_ARCHITECTURE.md`,
`docs/STAGE2_DATA_MODEL.md`, `docs/STAGE2_OPERATIONS.md`,
`docs/STAGE2_PILOT_REPORT.md`. Машиночитаемые результаты находятся в
`exports/stage2-pilot/` и намеренно не отслеживаются Git.
