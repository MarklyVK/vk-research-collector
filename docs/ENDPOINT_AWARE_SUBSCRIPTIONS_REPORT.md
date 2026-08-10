# Отчёт endpoint-aware subscriptions

## Реализованный контур

- `TokenLease` содержит secret только в памяти, fingerprint, точный method и время выдачи.
- PostgreSQL транзакционно координирует общий per-token RPS между CLI и worker.
- Код 6 ставит глобальный короткий cooldown; 9 и 29 блокируют только token+method с
  экспоненциальным ростом до max; 5/27/28 отключают токен целиком.
- Weighted round-robin проходит шесть job types. Недоступный method не блокирует другие,
  method deferral не расходует attempt, run получает `waiting_method_limit` и wakeup.
- Direct planner выбирает существующих публичных users по `vk_id`, учитывает TTL,
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

```bash
make backup PURPOSE=before-subscriptions-pilot
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector collection subscriptions capacity-preview
docker compose run --rm collector collection subscriptions pilot
docker compose run --rm collector collection subscriptions run --run-id RUN_ID --max-jobs 500
docker compose run --rm collector collection subscriptions status --run-id RUN_ID
```

Production cohort разрешается только после двух реальных JSON reports и backup:

```bash
COLLECTION_SUBSCRIPTIONS_ENABLED=true \
COLLECTION_SUBSCRIPTIONS_MAX_PER_USER=50 \
docker compose run --rm collector collection subscriptions plan

docker compose run --rm collector collection subscriptions run --run-id RUN_ID
```

Posts включаются отдельным rollout только после Gate B:

```bash
COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED=true \
docker compose run --rm collector collection subscriptions run --run-id RUN_ID
```

## Ограничения

Реальные VK tokens, real pilot и production deployment не запускались. Теоретический
Gate B не проходит лимит диска, поэтому массовый сбор posts сейчас запрещён. Reset
method limits точечный и требует `--yes`; массового reset токенов/data нет.
