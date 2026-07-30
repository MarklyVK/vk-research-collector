# Инвентаризация аварийного восстановления второго этапа

Дата проверки: 28.07.2026. Основной репозиторий:
`C:\data\vk-research-collector`.

| Worktree | Ветка | HEAD | Modified / staged / untracked | Относительно stage 1 | Последний commit | Подсистема |
|---|---|---|---|---:|---|---|
| `C:\data\vk-research-collector` | `feat/approved-data-collection` | `08ee042` | 0 / 0 / `collector.zip` | 0 behind / 12 ahead | `docs: add stage two final report` | интеграция stage 2 |
| `C:\data\vk-research-worktrees\agent-a-db` | `codex/agent-a-db` | `93b2732` | 0 / 0 / 0 | 11 behind / 1 ahead | `feat: add PostgreSQL persistence schema` | PostgreSQL stage 1 |
| `C:\data\vk-research-worktrees\agent-b-vk` | `codex/agent-b-vk` | `c53f312` | 0 / 0 / 0 | 11 behind / 1 ahead | `feat: add resilient VK group search` | VK client/search |
| `C:\data\vk-research-worktrees\agent-d-infra` | `codex/agent-d-infra` | `71d0774` | 0 / 0 / 0 | 11 behind / 1 ahead | `feat: add deployment and operations infrastructure` | Docker/CI/operations |
| `C:\data\vk-research-worktrees\agent-e-audit` | `codex/agent-e-audit` | `250d385` | 0 / 0 / 0 | 3 behind / 2 ahead | `fix: persist search checkpoint enum values` | audit/tests |

Незавершённых merge, cherry-pick, revert или rebase нет; `.git/index.lock` отсутствует.
Git-процессы не работали. Обнаружен только текущий процесс Codex.

В `C:\data` найден каталог `vk-research-worktrees` и архив
`vk-research-worktrees (2).zip`. Архив побайтно сравнен с четырьмя live-worktree:
полезные файлы совпадают. `collector.zip` распакован в отдельный recovery-каталог и
сравнен с основной веткой: совпадающие исходники не новее текущих, а текущая ветка
содержит дополнительные миграции, CI и тесты. Архив содержит `.env`, поэтому не
добавляется в Git и не используется как источник конфигурации.

Вложенный каталог `.git` есть только у основного репозитория. В worktree находятся
штатные `.git`-файлы, указывающие на `C:\data\vk-research-collector\.git\worktrees\...`.

Recovery-артефакты (исключены из Git):

- `backups/recovery/pre-stage2-recovery.bundle` — проверенный полный Git bundle;
- по три файла `*-unstaged.patch`, `*-staged.patch`, `*-untracked.txt` для каждого
  worktree; все patch-файлы пусты, в `main-untracked.txt` указан только `collector.zip`;
- `backups/stage2-recovery-20260728-082608Z.dump` — проверенный PostgreSQL backup.

## Повторная проверка после сбоя 30.07.2026

Основной репозиторий повторно подтверждён командой `git rev-parse --show-toplevel`:
`C:\data\vk-research-collector`. Перед продолжением создан и проверен полный bundle
`backups/recovery/session-20260730-161745Z/pre-second-recovery.bundle`, а также
сохранены status, staged/unstaged patch, список untracked, log и reflog. Stale lock,
незавершённых merge/cherry-pick/rebase и активных Git-процессов не было.

| Источник | Путь | Ветка | HEAD при аудите | Незакоммиченные / новые файлы | Подсистема | Качество | Нужно интегрировать |
|---|---|---|---|---|---|---|---|
| основной репозиторий | `C:\data\vk-research-collector` | `feat/approved-data-collection` | `49e86bc` | worker, integration test, final report / `collector.zip` | restart/resume | complete после тестов | да, commit `797194e` |
| live worktree | `C:\data\vk-research-worktrees\agent-a-db` | `codex/agent-a-db` | `93b2732` | нет / нет | PostgreSQL stage 1 | duplicate | нет |
| live worktree | `C:\data\vk-research-worktrees\agent-b-vk` | `codex/agent-b-vk` | `c53f312` | нет / нет | VK client/search | duplicate | нет |
| live worktree | `C:\data\vk-research-worktrees\agent-d-infra` | `codex/agent-d-infra` | `71d0774` | нет / нет | Docker/CI | duplicate | нет |
| live worktree | `C:\data\vk-research-worktrees\agent-e-audit` | `codex/agent-e-audit` | `250d385` | нет / нет | audit/tests | duplicate | нет |
| архив worktree | `C:\data\vk-research-recovery-worktrees-20260728-1323.zip` | неприменимо | неприменимо | caches и копии worktree | recovery archive | obsolete | нет |
| распакованный архив | `C:\data\vk-research-recovery-worktrees-20260728-1323` | повреждённые `.git`-ссылки | неприменимо | копии четырёх worktree | recovery archive | duplicate | нет |

Текущий статус компонентов после повторной проверки:

| Компонент | Статус | Проверка / примечание |
|---|---|---|
| Проектная документация | complete | requirements, architecture, data model, operations, privacy, recovery, gap и final report |
| Модели PostgreSQL и Alembic | complete | head `20260728_0004`, `alembic check` без новых операций |
| Collection run/jobs, locking, lease recovery | complete | PostgreSQL `SKIP LOCKED`, heartbeat и периодический recovery просроченного lease |
| VK client и token pool | complete | cooldown, invalid-token isolation, jitter retry, masking |
| Posts и attachments | complete | реальный сбор работает, fake pagination/upsert покрыты тестами |
| Members, profiles и subscriptions | complete | subscriptions реализованы и fake-tested, production scope выключен capacity gate |
| CLI, privacy CLI | complete | plan/run/status/pause/resume/retry/verify/summary и inspect/delete controls |
| Docker worker и disk guard | complete | новый image проверен stop/recreate/resume, `restart: unless-stopped` |
| Telegram | complete | best-effort и failure isolation |
| Unit/integration/fake VK | complete | 21 local и 23 container tests |
| CI | unverified | workflow существует; remote run невозможен без запрещённого `git push`, локальные эквиваленты passed |
| Pilot и capacity gate | complete | repilot 35 групп, прогноз 3,89 GiB, безопасные лимиты 100/200 |
| Operations documentation | complete | Debian 12, worker, monitoring, pause/resume и backup |
