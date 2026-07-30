# План реализации предметной области food_service

Дата аудита: 30.07.2026. Ветка: `feat/approved-data-collection`.

## Фактически найденная реализация до изменений

1. Разрешённые labels были зашиты в `classification.schemas.AllowedLabel` как
   `Literal` из трёх строк; отдельного Python enum предметных областей не было.
2. `group_labels.label` — строка под PostgreSQL CHECK из трёх значений;
   `search_keywords.subject` был свободной строкой. PostgreSQL enum используется для
   статусов классификации, search run, collection run и jobs, но не для labels.
3. Pydantic валидировал labels при полном импорте. Config не проверял полный набор
   областей, порядок и нормализованные дубли.
4. Поисковые задания формировались в `cli.app._run_search` из всего `keywords.yml`;
   subject-фильтра не было. PostgreSQL дедуплицировал candidates по `vk_id`, matches —
   по `(group_id, keyword_id)`.
5. `classification.service.import_classification` транзакционно импортировал
   неизменяемый batch, но удалял прежние labels и отвергал повторный импорт.
6. `CollectionQueue.plan` создавал один набор jobs на approved group; PostgreSQL UQ
   обеспечивал идемпотентность внутри run. Pilot содержал кортеж ровно из трёх labels.
7. Classification summary был динамическим по сохранённым labels, но не имел
   subject-фильтра. Stage 2 reports и pilot docs явно перечисляли три категории.
8. Жёсткий набор встречался в config tests, classification schemas и pilot selection.
9. При аудите PostgreSQL был healthy; `collector-worker` имел status `Exited (0)`,
   активного локального worker не было. Основной run в БД оставался `running`, locks=0.
10. Активных deploy/migration процессов не найдено. Git был чист по tracked-файлам;
    пользовательские `collector.zip` и `docs.zip` оставлены нетронутыми.

## Реализованный дизайн

- Единый реестр `subjects.py` с четырьмя машинными именами, русскими titles и
  описаниями.
- Строгая загрузка config: точный порядок областей, четыре titles, непустые слова,
  глобальные дубли после casefold/пробелов/`ё→е` запрещены.
- Migration 0005 расширяет два строковых CHECK и добавляет search/audit storage.
- Search `--subject food_service` использует отдельный plan key, checkpoints,
  candidate/match upsert и per-run known/new counters.
- Reclassification экспортирует полный snapshot, требует решение по каждому VK ID,
  проверяет SHA-256 контекста, не удаляет labels и пишет audit history.
- Независимый audit использует seed 20260730 и блокирует импорт/collection при
  недостигнутых thresholds.
- Incremental planner исключает baseline snapshot и уже completed jobs, фиксирует
  reason/source, дедуплицирует multi-label и требует audit/capacity gates.

## Операционный порядок

Migration и backup выполнены. Snapshot 37 407 групп подготовлен, но семантическая
разметка не завершена. До её завершения запрещены новый search, массовый импорт и
incremental run: это сохраняет воспроизводимость и основной collection run.
