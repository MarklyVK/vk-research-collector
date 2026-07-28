# Отчёт об аварийном восстановлении второго этапа

## Что найдено

Найдены основной репозиторий и четыре зарегистрированных Git worktree, перечисленные в
`STAGE2_RECOVERY_INVENTORY.md`. Все worktree чисты. Stash, tags и незавершённые Git
операции отсутствуют. Рабочая интеграционная ветка — `feat/approved-data-collection`.

`git fsck --full --no-reflogs --unreachable --lost-found` нашёл один недостижимый commit
`e3ecb16` (`feat: add deployment and operations infrastructure`). Его содержимое
сравнено с достижимым `71d0774`: достижимая версия исправляет deploy workflow и smoke
script, поэтому прежняя версия сознательно не восстанавливалась.

## Что восстановлено и интегрировано

Незакоммиченных полезных изменений после аварии не оказалось. Содержимое agent-веток
уже семантически интегрировано в stage 1 и затем унаследовано stage 2:

- `93b2732` — через интеграционный commit `a50e79a`;
- `c53f312` — через исправленную интеграцию `1687356` и последующие VK fixes;
- `71d0774` — через интеграционный commit `dd6c1b6`;
- audit-тесты и enum fix из `0f8d435`/`250d385` — в финальных commits
  `c4708a8`/`75b4095` с дополнительными исправлениями.

Ветка stage 2 уже содержала commits `be808e3..08ee042`: проектирование, миграции
0002/0003, PostgreSQL queue/worker, CLI/privacy, integration coverage, Docker runtime,
первый pilot, capacity gate и первоначальный итоговый отчёт. Слепые cherry-pick не
выполнялись: agent-ветки существенно отстают от интеграционной ветки.

## Что отклонено

- `e3ecb16` — устаревшая до amend версия `71d0774`.
- Содержимое `collector.zip` и `vk-research-worktrees (2).zip` — совпадает с live-кодом
  либо менее полно; `.env`, caches и runtime-экспорты не переносятся.
- `collector.zip` оставлен нетронутым как пользовательский untracked-файл.

## Состояние после восстановления

PostgreSQL healthy, migration head до продолжения — `20260728_0003`. Классификация
сохранена: approved=12 260, rejected=25 147, pending=0. Просроченных running jobs нет.
Первый pilot завершён, но capacity gate 13,54 GiB > 7 GiB не пройден; full run
`301fe7a5-be50-4b31-9640-147e067c4045` имеет 36 780 pending jobs и остаётся в
`paused_capacity_limit`. На момент аудита отдельный Compose service `collector-worker`
и обязательные privacy/gap/recovery-документы отсутствовали — эти пункты переданы в
gap analysis для завершения.

