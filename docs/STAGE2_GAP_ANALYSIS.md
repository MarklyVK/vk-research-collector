# Gap analysis второго этапа

Статусы отражают проверку фактического кода и запусков 28.07.2026.

| Требование | Статус | Доказательство / пробел |
|---|---|---|
| Recovery всех worktree, reflog/fsck и backup | complete | `STAGE2_RECOVERY_INVENTORY.md`, проверенный bundle и PostgreSQL dump |
| Сохранность классификации stage 1 | complete | Docker CLI: 12 260 approved, 25 147 rejected, 0 pending |
| Runs/jobs, SKIP LOCKED, lease recovery, idempotency | complete | `collection/queue.py`, `test_stage2_workflow.py` |
| Посты, attachments, members, profiles, subscriptions | complete | `collection/worker.py`, fake full-path integration test |
| Privacy CLI и транзакционный rollback | complete | `privacy.py`, `test_stage2_workflow.py` |
| Точное имя `collection_job_errors` | complete | Migration 0004 прошла на существующей/чистой БД, пять error rows сохранены |
| Привязка capacity report к лимитам plan/runtime | complete | Plan/config/report binding реализован и применён к full run 100/200 |
| Автономный Compose worker | complete | Service запущен, restart policy проверена через Docker inspect, UTC job logs видимы |
| Unit/static checks | complete | Ruff/mypy и 21 local test проходят; PostgreSQL tests выполняются в Docker |
| Чистая и существующая PostgreSQL migration | complete | 0001→0004, repeat upgrade и `alembic check` прошли |
| Уменьшенный реальный pilot 100 posts / 200 members | complete | Run `b09b119a-...`, 105 requests, прогноз 3,89 GiB, gate passed |
| Capacity-safe full run | complete | Run `9be2813e-...` запущен; groups/posts/members/users разрешены |
| Restart/resume автономного контейнера | complete | 124→190 completed после stop/start; последующий recreate продолжил тот же run |
| Remote CI | unverified | Push прямо запрещён; workflow проверяется локально |

Документ обновляется после каждого фактического gate; наличие файла без успешного
запуска не переводит пункт в `complete`.
