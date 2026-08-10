# Архитектура второго этапа

## Компоненты

PostgreSQL хранит очередь, lease, checkpoints и нормализованные данные. Compose service
`collector-worker` использует общий `httpx.AsyncClient`, глобальный semaphore и существующий
token pool с per-token RPS/cooldown. CLI планирует, запускает, приостанавливает,
возобновляет, проверяет и выводит статистику. Контроль диска запрещает тяжёлое
планирование выше 85% и ставит run на паузу выше 95%. Telegram — необязательный
best-effort канал; его сбой не влияет на транзакцию.

```mermaid
flowchart LR
    CLI["Русскоязычный CLI"] --> Q["PostgreSQL: runs и jobs"]
    Q --> W["collector-worker, restart unless-stopped"]
    W --> V["VK API client"]
    V --> T["Token pool и rate limiter"]
    W --> D["Нормализованные таблицы"]
    W --> M["Progress logs / Telegram"]
    G["Disk guard"] --> Q
    B["pg_dump / restore check"] --> Q
```

## Выполнение задания

```mermaid
sequenceDiagram
    participant W as Worker
    participant DB as PostgreSQL
    participant VK as VK API
    W->>DB: recover expired leases
    W->>DB: SELECT job FOR UPDATE SKIP LOCKED
    DB-->>W: running job + lease
    loop каждая страница
        W->>VK: запрос с разрешёнными fields
        VK-->>W: публичные данные или категория ошибки
        W->>DB: upsert rows + checkpoint + metrics (одна транзакция)
    end
    W->>DB: completed / skipped / retry_wait
```

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> running: lease
    retry_wait --> running: next_attempt_at
    running --> completed
    running --> skipped: terminal access/privacy
    running --> retry_wait: transient error
    running --> failed: attempts exhausted
    running --> pending: lease expired
    pending --> paused: run paused
    paused --> pending: resume
    pending --> cancelled
```

```mermaid
flowchart TD
    A["approved group_candidate"] --> GM["group collection state"]
    A --> P["group_posts"]
    P --> PA["post_attachments"]
    A --> MB["group_memberships"]
    MB --> U["vk_users"]
    U --> S["user_group_subscriptions"]
    R["collection_run"] --> J["collection_jobs"]
    J --> E["collection_job_errors"]
    R --> MB
    R --> S
```

## Lifecycle run, retry и recovery

```mermaid
stateDiagram-v2
    [*] --> planned
    planned --> running: capacity gate passed + worker
    running --> paused: operator pause
    running --> paused_no_tokens: tokens unavailable
    running --> paused_capacity_limit: disk stop
    paused --> running: resume
    paused_no_tokens --> running: tokens restored + resume
    running --> completed: all jobs terminal
    running --> completed_with_errors: failed jobs remain
    planned --> cancelled
    running --> failed: fatal run error
```

```mermaid
flowchart LR
    E["transient VK/network error"] --> B1["1 min"] --> B2["5 min"] --> B3["15 min"]
    B3 --> B4["1 hour"] --> B5["6 hours"] --> F["failed"]
    R["rate limit"] --> C["cooldown current token"] --> N["next valid token"]
    A["invalid token"] --> X["disable only this token"] --> N
    P["private/deleted/permission"] --> S["skipped without retry"]
```

```mermaid
flowchart TD
    C["container/host interruption"] --> L["lease remains in PostgreSQL"]
    L --> D{"lease expired?"}
    D -->|yes| P["recover to pending"]
    D -->|no| W["wait for active worker"]
    P --> K["claim with SKIP LOCKED"]
    K --> Q["continue from page checkpoint"]
    Q --> U["idempotent upsert / unique constraints"]
```

## Надёжность

Lease содержит `locked_at`, `locked_by` и heartbeat. Перед захватом worker возвращает
просроченные jobs в pending. Retry использует 1, 5, 15, 60 и 360 минут с jitter;
permission/private/deleted не повторяются. SIGTERM прекращает захват новых jobs,
завершает текущую page transaction и закрывает HTTP/DB connections. Ни одна операция
не держит транзакцию на весь scope.

Plan-key и capacity report содержат одинаковую collection-конфигурацию; несовпадение
лимитов блокирует worker до обращения к VK. Backup создаётся перед schema/pilot/main run,
проверяется `pg_restore --list` и не коммитится.

После сбоя `collector-worker` запускается Docker Compose автоматически и выбирает
full/incremental/subscriptions/subscription_posts run только с `capacity_gate=passed`.
Pilot автономно не выбирается. Ручной путь — `collection resume`, затем
`collection run --run-id ... --until-idle`; checkpoint исключает дубли уже сохранённых
страниц. На Windows это требует запущенного Docker Desktop; на Debian Docker включается
через systemd.

## Расширение food_service

```mermaid
flowchart LR
    S["Snapshot 37 407 групп"] --> R["Семантическая reclassification"]
    R --> A["Независимый аудит, seed 20260730"]
    A --> I["Merge-only import и audit history"]
    I --> V["Отдельный VK search: food_service"]
    V --> C["Классификация новых групп по 4 labels"]
    C --> G["Capacity gate"]
    G --> N["Incremental collection run"]
    O["Основной run snapshot"] -->|"исключение уже включённых group IDs"| N
```

Единый реестр областей расположен в `vk_collector.subjects`. PostgreSQL CHECK
ограничивает `search_keywords.subject` и `group_labels.label`. `search_run_groups`
даёт per-run дедупликацию и статистику known/new, а `classification_reviews` хранит
неизменяемую историю повторных решений.

## Endpoint-aware scheduler

`VKClient.call(method, params)` получает lease только для точного `method`. Секрет живёт
только в lease процесса; PostgreSQL хранит fingerprint, глобальное состояние токена и
отдельные method states. Выдача RPS координируется транзакционной блокировкой строки
token state, поэтому параллельные CLI/worker процессы не превышают общий лимит.

Типы jobs отображаются на методы без объединения в семейства. Scheduler циклически
обходит `refresh_group`, `collect_group_posts`, `collect_group_members`,
`refresh_user_profile`, `collect_user_subscriptions` и
`collect_subscription_group_posts`; priority применяется только внутри типа.

Канонический поток данных:

```text
approved membership -> vk_user -> groups.get extended=1
  -> vk_communities + user_group_subscriptions
  -> unique collect_subscription_group_posts -> group_posts + post_attachments
```

Subscription communities не попадают в поисковую классификацию. `group_posts`
ссылается на `vk_communities`; необязательная обратная связь с `group_candidates`
сохраняется для совместимости.

`next_probe_at` — не декоративное поле: после его наступления worker транзакционно
переносит его вперёд и получает единственный probe lease, хотя `blocked_until` ещё не
истёк. Успех очищает method state, повторный limit увеличивает backoff. Run в
`waiting_method_limit` остаётся видим автономному worker и возвращается в `running`
после `next_wakeup_at`; method deferral уменьшает обратно технический claim attempt.
