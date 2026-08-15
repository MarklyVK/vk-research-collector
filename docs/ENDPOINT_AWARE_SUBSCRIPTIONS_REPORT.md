# Отчёт endpoint-aware subscriptions

## Реализованный контур

- `TokenLease` содержит secret только в памяти, fingerprint, точный method и время выдачи.
- PostgreSQL транзакционно координирует общий per-token RPS между CLI и worker.
- Код 6 ставит глобальный короткий cooldown; 9 и 29 блокируют только token+method с
  экспоненциальным ростом до max; 5/27/28 отключают токен целиком.
- После `next_probe_at` PostgreSQL под блокировкой строки резервирует одну пробу. Успех
  очищает блок, повторный 9/29 увеличивает backoff. События разных методов в заданном
  окне дают сохраняемый короткий global cooldown; повторы одного метода не эскалируют.
- Weighted round-robin проходит шесть job types. Недоступный method не блокирует другие,
  method deferral не расходует attempt, run получает `waiting_method_limit` и wakeup.
- Direct planner выбирает существующих публичных users по `vk_id`, независимо от TTL
  профиля и с учётом собственного subscription TTL,
  фиксирует hash snapshot и создаёт jobs идемпотентно.
- `groups.get extended=1` сохраняет 50 (или 100) community objects, связи и checkpoint
  одной транзакцией. Код 260 фиксируется как private terminal skip. Усечённый snapshot
  не деактивирует прежние связи.
- Уникальная community создаёт одну `collect_subscription_group_posts` job на run;
  сохраняется до 20 posts и нормализованные attachments без binary/raw response.

## Схема и migration

Migration `20260810_0006_endpoint_aware_subscriptions.py` добавляет:

- `vk_token_states`;
- `vk_token_method_states`;
- `vk_communities`;
- `user_subscription_states`;
- `collection_runs.next_wakeup_at` и enum `waiting_method_limit`;
- FK subscription → community;
- `group_posts.community_vk_id`, nullable candidate FK с `ON DELETE SET NULL` и индекс.

Migration `20260810_0007_subscription_safety.py` добавляет
`community_post_collection_states`: общий для candidate/subscription path TTL,
last attempt/success, run, error/private/unavailable и collected count. Backfill не
переписывает posts и оставляет существующие communities доступными к безопасному refresh.

Backfill выполняется SQL-порциями по 10 000. На рабочей локальной копии связано
698 915 posts и создано 51 334 canonical communities; orphan subscriptions/posts и
дубли owner+post/user+community равны нулю. Повторный upgrade идемпотентен. Чистая БД
прошла всю цепочку 0001→0006 и `alembic check`.

## Capacity preview

Для cohort 10 000 users × 50 теоретический Gate A даёт до 500 000 links/communities и
640 000 000 bytes. Консервативный Gate B даёт до 10 000 000 posts и 23 040 000 000
bytes, что превышает safe limit 7 GiB. Поэтому `production_allowed=false`; нужен
реальный Pilot A, измерение unique-community dedupe и затем отдельный Pilot B.

## Безопасный запуск

Gate A, production subscriptions, Gate B и production posts — четыре разных run.
Изменение flag/TTL/лимита требует нового run и нового совпадающего report:

```bash
COLLECTION_SUBSCRIPTIONS_ENABLED=true \
COLLECTION_SUBSCRIPTIONS_MAX_PER_USER=50 \
COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED=false \
docker compose run --rm collector collection subscriptions pilot

docker compose run --rm collector collection subscriptions plan
docker compose run --rm collector collection capacity-apply \
  --run-id SUBSCRIPTIONS_RUN_ID \
  --source /app/exports/stage2-pilot/subscription-gate-a.json \
  --backup /app/backups/BEFORE_SUBSCRIPTIONS.dump
docker compose run --rm collector collection subscriptions run --run-id SUBSCRIPTIONS_RUN_ID
```

Затем включается posts scope и выполняется отдельный Gate B:

```bash
COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED=true \
docker compose run --rm collector collection subscriptions posts-pilot \
  --source-run-id PILOT_A_RUN_ID
docker compose run --rm collector collection subscriptions posts-plan \
  --source-run-id SUBSCRIPTIONS_RUN_ID
docker compose run --rm collector collection capacity-apply \
  --run-id POSTS_RUN_ID \
  --source /app/exports/stage2-pilot/subscription-gate-b.json \
  --backup /app/backups/BEFORE_SUBSCRIPTION_POSTS.dump
docker compose run --rm collector collection subscriptions run --run-id POSTS_RUN_ID
```

## Ограничения

Реальные VK tokens, Pilot A/B и production deployment в этой реализации не запускались.
Сгенерированных измеренных разрешающих reports нет, поэтому оба production gate закрыты.
Теоретический Gate B превышает лимит диска и ничего не разрешает. Reset
method limits точечный и требует `--yes`; массового reset токенов/data нет.
# Дополнение 2026-08-15

Коды VK 9 и 29 теперь сохраняются без потери исходного кода, а method state содержит
successful requests, суммарный cooldown, последний success/error и next probe.
Автономный scheduler больше не выбирает только последний run: он round-robin
обслуживает все разрешённые non-pilot run, пропуская заблокированный метод.
Ручная CLI-команда сброса cooldown удалена.
