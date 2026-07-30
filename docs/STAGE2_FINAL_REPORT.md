# Итоговый отчёт второго этапа

Дата первоначального завершения: 28.07.2026. Повторная recovery-проверка: 30.07.2026.
Ветка: `feat/approved-data-collection`.

## Итоговый статус

Техническая реализация второго этапа, миграции, fake/integration-тесты, Docker,
реальный pilot, capacity gate и запуск автономного worker завершены. Полный сетевой
сбор всех 12 260 approved-групп закономерно продолжается после окончания инженерной
сессии; это не скрывается и не считается уже завершённым сбором данных.

Основной run: `9be2813e-e1de-4ac9-bc07-7d92ac82438c`. Service
`collector-worker` работает независимо от Codex с `restart: unless-stopped`, хранит
очередь, lease, checkpoints и прогресс в PostgreSQL. Старый опасный run
`301fe7a5-be50-4b31-9640-147e067c4045` сохранён в `paused_capacity_limit` и не
используется.

После повторного аварийного запуска 30.07 восстановлен незакоммиченный lease-recovery
loop: worker периодически возвращает просроченные jobs во время собственной работы,
а не только один раз при старте. Исправление зафиксировано в `797194e`, новый image
развёрнут, тот же основной run продолжен.

## 1–6. Recovery и завершённая реализация

Найдены основной репозиторий и четыре worktree:

| Worktree | Ветка | HEAD при recovery | Подсистема |
|---|---|---|---|
| `C:\data\vk-research-collector` | `feat/approved-data-collection` | `08ee042` | интеграция stage 2 |
| `C:\data\vk-research-worktrees\agent-a-db` | `codex/agent-a-db` | `93b2732` | PostgreSQL |
| `C:\data\vk-research-worktrees\agent-b-vk` | `codex/agent-b-vk` | `c53f312` | VK client/search |
| `C:\data\vk-research-worktrees\agent-d-infra` | `codex/agent-d-infra` | `71d0774` | Docker/CI/operations |
| `C:\data\vk-research-worktrees\agent-e-audit` | `codex/agent-e-audit` | `250d385` | audit/tests |

Все worktree были чистыми. В основном каталоге оставлен нетронутым пользовательский
untracked `collector.zip`; он содержит `.env` и поэтому не импортировался и не
коммитился. Архив `vk-research-worktrees (2).zip` и распакованная копия collector
побайтно сравнены с live-кодом: полезных более новых изменений нет.

`git fsck` нашёл один unreachable commit `e3ecb16`. Это старая версия достижимого
`71d0774`; в достижимом commit исправлены deploy workflow и smoke script, поэтому
`e3ecb16` обоснованно отброшен. Незавершённых merge/cherry-pick/rebase, stale lock,
stash и потерянных незакоммиченных исходников не было.

Повторный `git fsck` 30.07 дополнительно показал `21b378c` и `14010bf`: это
предшественники достижимых, исправленных commits `4f8e938` и `e2a6371`. Они также не
восстанавливались. Проверен точный архив
`C:\data\vk-research-recovery-worktrees-20260728-1323.zip`; новых исходников в нём
нет.

Agent-ветки ранее уже были семантически объединены в stage 1 commits `a50e79a`,
`1687356`, `dd6c1b6`, `c4708a8` и `75b4095`; повторный blind cherry-pick не выполнялся.
До продолжения stage 2 уже существовали проектирование, миграции 0002/0003,
PostgreSQL queue/worker, VK scopes, CLI/privacy, integration tests, Docker runtime,
первый pilot и первоначальный capacity gate.

После recovery добавлены точное имя error-таблицы и migration 0004, привязка capacity
report к полному набору runtime-лимитов, безопасные defaults 100 posts/200 members,
отдельный автономный Compose worker, выбор только runnable capacity-passed run,
персистентный retry-wait loop, UTC progress/job logging, уведомления 10%, disk warning,
pause/resume, jitter 0,9–1,1 и новые тесты. Документация recovery, privacy,
архитектуры, capacity и operations приведена в соответствие коду.

## 7–10. PostgreSQL, классификация и миграции

Stage 2 использует таблицы `collection_runs`, `collection_jobs`,
`collection_job_errors`, `group_collection_states`, `group_posts`,
`post_attachments`, `vk_users`, `group_memberships` и
`user_group_subscriptions`, переиспользуя `group_candidates` и `group_labels`.

Цепочка Alembic: `20260728_0001` → `20260728_0002_stage2_collection` →
`20260728_0003_vk_user_natural_key` → `20260728_0004_collection_job_errors`.
Migration 0004 переименовывает прежнюю таблицу и индексы без потери истории: все пять
error rows сохранились. `upgrade head`, повторный upgrade, `current`, `heads`,
`alembic check` прошли на рабочей и отдельной чистой БД.

Данные первого этапа сохранены: 37 407 групп, approved=12 260,
rejected=25 147, pending=0; labels food_delivery=6 419,
customer_acquisition=4 464, tender_support=1 382, multi-label=5.

До pilot были точно идентифицированы и транзакционно удалены 12 integration fixtures:
шесть approved и шесть rejected, отсутствовавших в `exports/classification-final`.
Тесты переведены на отдельную БД/savepoint rollback; удаления по диапазону ID или
времени не применялись.

## 11. Проверки

Финальный обязательный прогон после последних изменений успешен:

- `ruff check .` — passed;
- `ruff format --check .` — 67 files already formatted;
- `mypy src` — 27 source files без ошибок;
- local `pytest -q` — 21 passed, 2 PostgreSQL tests штатно skipped;
- `docker compose config` и `docker compose build` — passed;
- PostgreSQL healthy, `alembic current` — `20260728_0004 (head)`,
  `alembic check` — no new upgrade operations;
- Docker/PostgreSQL `pytest -q` — 23 passed.

Миграции чистой и рабочей БД, fake full path, queue lease recovery, checkpoints,
restart, retries/token cooldown, snapshot semantics, dedupe, privacy rollback,
Telegram failure isolation и capacity binding проверены тестами.

GitHub Actions содержит Ruff, format, mypy, unit/integration/fake smoke, Docker build,
clean-DB Alembic и secret scan. Remote workflow не запускался, потому что пользователь
прямо запретил `git push`; все доступные шаги повторены локально.

## 12–16. Реальный pilot и capacity gate

Первый pilot `4c539596-288a-4141-a08a-f3e6887ad1b0` с лимитами 200/1000 завершился,
но дал небезопасный прогноз 14 537 357 656 байт (13,54 GiB) против safe limit
7 516 192 768 байт. Его full run оставлен на паузе.

Новый изолированный repilot `b09b119a-3e5b-408e-a6ad-a327888c57fd` использовал seed
20260728, 35 approved-групп и чистый stage-2 baseline:

| Показатель | Результат |
|---|---:|
| duration | 70,5 секунды |
| completed / skipped / failed jobs | 4 413 / 5 / 0 |
| VK requests / retries | 105 / 0 |
| groups | 35 |
| posts / attachments | 2 367 / 4 432 |
| memberships / users | 4 319 / 4 313 |
| subscriptions | 0 |
| БД до / после | 85 228 567 / 94 206 999 байт |
| прирост | 8 978 432 байта |

Пять skips — конечная официальная ошибка VK 15 для скрытых участников. Дубли posts,
memberships и subscriptions, rejected jobs, retries, failed jobs и зависшие locks — 0.
Счётчики upsert этого запуска зарегистрировали `rows_inserted=0` и
`rows_updated=11034`; это операционная метрика обработанных upsert, а фактический
прирост сущностей отдельно и точнее отражён сравнением таблиц до/после выше.

Прогноз с коэффициентом 1,30 для индексов/WAL/резерва — 4 173 749 973 байта
(3,89 GiB), ниже 70% от 10 GiB. Gate `passed` только для конфигурации:
groups, posts≤100, members≤200 и минимальные public user profiles. Public
subscriptions реализованы и fake-tested, но отключены до отдельного pilot/capacity
gate; scraping, raw VK JSON и binary media не используются.

## 17–22. Основной run и автономный worker

Capacity-safe run: `9be2813e-e1de-4ac9-bc07-7d92ac82438c`.

Контрольный снимок 28.07.2026 после запуска:

| Показатель | Значение |
|---|---:|
| run status | running |
| completed / pending / running | 3 392 / 33 385 / 3 |
| retry / failed | 0 / 0 |
| API requests | 3 392 |
| disk used | 83,5% |
| groups в БД | 37 407 |
| posts / attachments | 3 969 / 7 261 |
| memberships / users | 12 316 / 12 285 |
| subscriptions | 0 |
| runs / jobs / errors | 4 / 85 950 / 5 |

Счётчики меняются во время чтения, потому что worker продолжает работу. На снимке ещё
шла первая волна refresh_group; поэтому объёмы posts/members/users пока соответствуют
первому pilot в рабочей БД.

Service `collector-worker` имеет фактическую restart policy
`{"Name":"unless-stopped","MaximumRetryCount":0}`. Проверка stop/start:
перед остановкой было 124 completed, после запуска стало 190; последующий rebuild и
recreate продолжил тот же run и довёл счётчик выше 1 000. Running locks корректно
закрылись при штатной остановке, API counter не сбросился, дубли не появились.
После перезагрузки Windows worker продолжится после запуска Docker Desktop; на Debian
нужны enabled Docker service и Compose unit из operations guide.

Повторный контрольный stop/recreate на новом image 30.07.2026:

| Показатель | До recreate | Через 8 секунд после recreate |
|---|---:|---:|
| completed jobs | 12 361 | 12 380 |
| pending jobs | 24 413 | 24 394 |
| retries | 5 | 5 |
| failed / duplicates / rejected jobs | 0 | 0 |

Следующий согласованный снимок во время волны постов: 12 452 completed, 24 322
pending, 6 running, 5 retries; 18 329 posts, 31 989 attachments, 12 316 memberships,
12 285 users и 0 subscriptions. Размер БД — 165 198 871 байт, disk used — 83,0%.
Значения продолжают меняться, потому что worker работает.

Disk warning=85%, stop=95%. Текущие 83,5% ниже warning, но близки к нему; worker
проверяет диск перед тяжёлыми jobs, однократно уведомляет о warning и ставит run на
паузу при stop threshold.

## 23. Резервные копии

Все dump-файлы ненулевые и проверены через `pg_restore --list`; recovery bundle
проверен через `git bundle verify`:

| Файл | Размер, байт | Назначение |
|---|---:|---|
| `backups/before-classification-import-20260727-232746Z.dump` | 20 915 232 | stage 1 до импорта |
| `backups/stage2-test-cleanup-20260728-000455Z.dump` | 21 519 079 | до очистки fixtures |
| `backups/stage2-pilot-20260728-002754Z.dump` | 21 551 090 | до первого pilot |
| `backups/stage2-recovery-20260728-082608Z.dump` | 25 012 122 | до recovery-миграций |
| `backups/stage2-repilot-20260728-083636Z.dump` | 21 578 639 | до repilot |
| `backups/stage2-pre-full-20260728-083901Z.dump` | 25 012 235 | до нового full run |
| `backups/recovery/pre-stage2-recovery.bundle` | проверен | все Git refs до продолжения |
| `backups/recovery/session-20260730-161745Z/pre-second-recovery.bundle` | 314 074 | все Git refs и незакоммиченный checkpoint перед повторной recovery |

В `backups/recovery/` также сохранены staged/unstaged patch и untracked inventory для
каждого worktree. Каталог исключён из Git.

## 24. Commits

Существовавшая цепочка stage 2: `be808e3`, `dfc26ea`, `6a2398b`, `24acc35`,
`ac1dcd1`, `d79c960`, `dbe5e43`, `9bcd0a3`, `dc544aa`, `becd4f1`, `e927ed6`,
`08ee042`.

После аварийного восстановления созданы:

- `ebf858c chore: recover interrupted stage two work`;
- `4f8e938 fix: enforce safe autonomous collection`;
- `e2a6371 feat: add autonomous worker observability`;
- `6672f74 fix: jitter transient VK retries`;
- `49e86bc docs: record completed stage two launch`;
- `797194e fix: recover expired leases during worker lifetime`;
- финальный documentation commit повторной recovery с этим отчётом.

`git push` не выполнялся.

## 25. Известные ограничения

- Полный сетевой run ещё выполняется; его нельзя представлять как завершённый.
- Subscriptions отключены до отдельного capacity gate.
- Remote GitHub Actions не запускался без push; workflow проверен локальными
  эквивалентами.
- Текущее заполнение локального диска близко к warning threshold; disk guard активен.
- Telegram необязателен и best-effort; отсутствие или сбой Telegram не останавливает
  сбор.
- `restart: unless-stopped` начинает работать после запуска Docker daemon/Desktop,
  а не до него.

## 26. Наблюдение, пауза и продолжение

Выполнять из `C:\data\vk-research-collector`:

```powershell
docker compose ps
docker compose logs -f collector-worker
docker compose run --rm collector collection status --run-id 9be2813e-e1de-4ac9-bc07-7d92ac82438c
docker compose run --rm collector collection summary
docker compose run --rm collector collection pause --run-id 9be2813e-e1de-4ac9-bc07-7d92ac82438c
docker compose run --rm collector collection resume --run-id 9be2813e-e1de-4ac9-bc07-7d92ac82438c
```

После ручного `pause` worker ожидает. После `resume` он продолжает тот же run. Для
возврата service после явного `docker compose stop collector-worker`:

```powershell
docker compose up -d collector-worker
```

## Дополнение 30.07.2026: expansion food_service

Добавлены единый реестр четырёх областей, 28 поисковых фраз «Общепит», migration
`20260730_0005`, per-run search statistics, merge-only reclassification с audit history,
фиксированный независимый аудит и incremental planning вне snapshot основного run.

Создан backup `stage2-food-service-migration-20260730-175811Z.dump` (178 051 201 байт),
проверенный `pg_restore --list`. Migration прошла на чистой БД, восстановленной копии
с 37 407 группами и рабочей БД; повторный upgrade и `alembic check` успешны. Основной
run сохранил 36 780 jobs, дубли и rejected jobs отсутствуют.

Подготовлен runtime snapshot reclassification operation
`food-service-20260730-d78d7615475b`: 37 407 групп, SHA-256
`30664c236d1a054f4d1467255acc71c0ba2e73c4e33a29f3d2cc9b8b66e5720c`. Семантических
решений завершено 0, поэтому новый search, аудит, импорт и incremental run намеренно
не запускались. Полная готовность expansion пока не заявляется.
