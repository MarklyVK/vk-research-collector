# Архитектура второго этапа

## Компоненты

PostgreSQL хранит очередь, lease, checkpoints и нормализованные данные. Один лёгкий
async worker использует общий `httpx.AsyncClient`, глобальный semaphore и существующий
token pool с per-token RPS/cooldown. CLI планирует, запускает, приостанавливает,
возобновляет, проверяет и выводит статистику. Контроль диска запрещает тяжёлое
планирование выше 85% и ставит run на паузу выше 95%. Telegram — необязательный
best-effort канал; его сбой не влияет на транзакцию.

```mermaid
flowchart LR
    CLI["Русскоязычный CLI"] --> Q["PostgreSQL: runs и jobs"]
    Q --> W["Async worker, concurrency <= 3"]
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
    J --> E["collection_errors"]
    R --> MB
    R --> S
```

## Надёжность

Lease содержит `locked_at`, `locked_by` и heartbeat. Перед захватом worker возвращает
просроченные jobs в pending. Retry использует 1, 5, 15, 60 и 360 минут с jitter;
permission/private/deleted не повторяются. SIGTERM прекращает захват новых jobs,
завершает текущую page transaction и закрывает HTTP/DB connections. Ни одна операция
не держит транзакцию на весь scope.

Backup создаётся перед schema/pilot/main run, проверяется `pg_restore --list` и не
коммитится. После сбоя контейнер запускается обычной командой `collection resume`, затем
`collection run --run-id ... --until-idle`; checkpoint исключает повтор уже сохранённых
страниц.
