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

