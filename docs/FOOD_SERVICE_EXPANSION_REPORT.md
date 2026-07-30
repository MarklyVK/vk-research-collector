# Отчёт расширения «Общепит»

Дата: 30.07.2026. Статус: инженерная реализация готова, data operations ожидают
семантической разметки и не выдаются за завершённые.

## Реализовано

- Машинное имя: `food_service`; русское название: «Общепит».
- Граница: действующее заведение непосредственно готовит и продаёт еду/напитки;
  поставщики, оборудование, каталоги, вакансии, рецепты и агрегаторы не входят.
- Добавлено 28 устойчиво упорядоченных ключевых фраз без точных/нормализованных дублей.
- Migration `20260730_0005` расширила CHECK `group_labels` и `search_keywords`, добавила
  search counters, `search_run_groups` и `classification_reviews`.
- CLI поддерживает subject search/summary, reclassification prepare/validate/import,
  audit prepare/validate и gated incremental plan.
- Основной run `9be2813e-e1de-4ac9-bc07-7d92ac82438c` не изменён: 36 780 jobs,
  completed checkpoints не сбрасывались, verify показывал нулевые дубли/rejected jobs.

## Reclassification

- Operation: `food-service-20260730-d78d7615475b`.
- Snapshot: 37 407 групп.
- SHA-256: `30664c236d1a054f4d1467255acc71c0ba2e73c4e33a29f3d2cc9b8b66e5720c`.
- Завершено решений: 0; получили `food_service`: 0; rejected→approved: 0.

## Ещё не выполнено

- Отдельный VK search `food_service`: не запускался до завершения snapshot review.
- Классификация новых групп по четырём labels: не запускалась.
- Независимый аудит и quality thresholds: не измерялись.
- Массовый import: не выполнялся.
- Incremental run ID/jobs/прогноз: отсутствуют до audit и capacity gate.

## Проверки и backup

Backup: `backups/stage2-food-service-migration-20260730-175811Z.dump`, 178 051 201
байт, `pg_restore --list` passed. Migration прошла на чистой БД, restored copy и
рабочей БД; повторный upgrade и `alembic check` passed. Unit/local suite: 34 passed,
10 integration tests skipped вне test DB. Изолированный PostgreSQL suite: 4 passed.

Полные итоговые counts, audit metrics, disk forecast и incremental run будут заполнены
только фактическими значениями после завершения data operations.
