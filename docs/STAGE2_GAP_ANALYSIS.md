# Gap analysis второго этапа

Статусы отражают проверку фактического кода и запусков 28.07.2026.

| Требование | Статус | Доказательство / пробел |
|---|---|---|
| Recovery всех worktree, reflog/fsck и backup | complete | `STAGE2_RECOVERY_INVENTORY.md`, проверенный bundle и PostgreSQL dump |
| Сохранность классификации stage 1 | complete | Docker CLI: 12 260 approved, 25 147 rejected, 0 pending |
| Runs/jobs, SKIP LOCKED, lease recovery, idempotency | complete | `collection/queue.py`, `test_stage2_workflow.py` |
| Посты, attachments, members, profiles, subscriptions | complete | `collection/worker.py`, fake full-path integration test |
| Privacy CLI и транзакционный rollback | complete | `privacy.py`, `test_stage2_workflow.py` |
| Точное имя `collection_job_errors` | partial | Реализация и migration 0004 подготовлены, Docker migration ещё не проверена |
| Привязка capacity report к лимитам plan/runtime | partial | Код и тест конфигурации подготовлены, реальный repilot ещё не выполнен |
| Автономный Compose worker | partial | Service и команда `collection worker` подготовлены, контейнер ещё не запущен |
| Unit/static checks | complete | Ruff/mypy и 20 local tests проходят; 2 PostgreSQL tests локально skipped |
| Чистая и существующая PostgreSQL migration | unverified | Требуется Docker build, upgrade текущей и отдельной чистой БД |
| Уменьшенный реальный pilot 100 posts / 200 members | missing | Первый pilot 200/1000 не прошёл capacity gate |
| Capacity-safe full run | missing | Старый run безопасно paused; новый run ещё не разрешён |
| Restart/resume автономного контейнера | unverified | Fake restart прошёл; требуется остановка/запуск Compose worker |
| Remote CI | unverified | Push прямо запрещён; workflow проверяется локально |

Документ обновляется после каждого фактического gate; наличие файла без успешного
запуска не переводит пункт в `complete`.

