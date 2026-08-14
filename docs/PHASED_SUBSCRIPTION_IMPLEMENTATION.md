# Многофазная кампания подписок

## Диагноз

Низкая фактическая скорость была вызвана главным образом quota/cooldown метода
`groups.get`, а не CPU. Старая реализация дополнительно запрашивала `extended=1` и
писала каждую группу и связь отдельным UPSERT. Это смешивало discovery с metadata,
создавало около 100 SQL statements на пользователя при лимите 50 и увеличивало
вероятность deadlock при пересекающихся сообществах.

## Новый pipeline

```text
subscription campaign
  -> фиксированный snapshot пользователей
  -> cohort не более 10 000 пользователей
  -> groups.get extended=0 (только ID)
  -> bulk stub vk_communities (metadata_updated_at = NULL)
  -> bulk user_group_subscriptions
  -> successful / terminal / truncated state каждого snapshot user
  -> завершение всех discovery cohort
  -> DISTINCT current vk_group_id
  -> groups.getById пакетами
  -> metadata_updated_at только для полученных communities
  -> завершение metadata phase
  -> отдельный, по умолчанию выключенный Gate B для постов
```

«Все подписки» здесь означает завершение каждого пользователя snapshot в рамках
действующей политики максимум 50 связей. Если VK сообщает больше, сохраняются
`total_reported`, первые 50 ID и `is_truncated=true`. Такой пользователь завершён
для текущей политики, но результат не является полным списком его реальных
подписок. Увеличение лимита требует нового Pilot A, capacity report, backup и
отдельного решения оператора.

## Контракт VK

ID-only вызов проверен по официальной схеме VK API 5.199:
[`groups.get`](https://github.com/VKCOM/vk-api-schema/blob/master/groups/methods.json)
возвращает `count` и integer IDs при `extended=0`. Для `groups.getById` официальная
схема не задаёт `maxItems` для `group_ids`; поэтому `100` — консервативный
настраиваемый default проекта, а не заявленный официальный максимум VK.
Коды 9 и 29 различаются и сохраняются в PostgreSQL вместе с временем ошибки,
cooldown, probe и числом успешных запросов.

## Durable state machine

`collection_campaigns` хранит фазу, статус, `snapshot_at`, верхнюю границу ID,
immutable configuration hash, курсоры cohort и metadata, wakeup и ошибку. Частичный
уникальный индекс PostgreSQL запрещает две активные кампании одного типа с одной
конфигурацией. `collection_runs.campaign_id` nullable, поэтому исторические run
остаются читаемыми. Миграция не переписывает `collection_jobs`.

Переход discovery -> metadata разрешён только когда нет active/failed discovery
jobs и каждый eligible user имеет successful (включая truncated) либо terminal
state. Transient error остаётся retryable и блокирует переход. После рестарта
planner продолжает от durable курсоров; повторное планирование использует ту же
кампанию и уникальные job constraints.

## Scheduler и cooldown

Автономный worker получает все разрешённые non-pilot run, обрабатывает их
round-robin квантами и использует один endpoint-aware token pool. Блокировка
`groups.get` не мешает `users.get`, `groups.getById` или разрешённому `wall.get`.
Когда доступных методов нет, run/campaign получают `waiting_method_limit` и
`next_wakeup_at`; worker остаётся healthy. Ручной reset cooldown удалён из CLI:
он не увеличивает quota и провоцирует повторные ограничения.

## CLI

```bash
collector collection backlog --json
collector collection campaign plan
collector collection campaign plan --apply
collector collection campaign status [--campaign-id UUID]
collector collection campaign metadata-preview --campaign-id UUID
collector collection campaign pause --campaign-id UUID
collector collection campaign resume --campaign-id UUID
collector collection method-limits [--method groups.get]
collector collection repair-stale-leases
collector collection repair-stale-leases --confirm
```

`collection backlog` читает канонические state-таблицы. Исторические jobs выводятся
отдельно как `rows` и `distinct_entities`: ни одно из этих чисел автоматически не
считается реальным backlog. Старые paused heavy runs не возобновляются автоматически.
Repair без `--confirm` является read-only preview; подтверждение возвращает только
истёкшие running lease в pending, фиксируя `lease_expired_recovered`, и не удаляет
terminal jobs, связи, communities или историю. ETA остаётся `null`, пока в текущем
campaign нет достаточного измеренного healthy throughput и cooldown duty cycle:
операционный отчёт не выдумывает скорость из исторических job rows.

## Безопасный rollout (не выполнен)

1. Получить `collection backlog --json`, campaign status и method-limits.
2. Проверить диск; при 85% остановить rollout.
3. Создать `pg_dump -Fc`, проверить `pg_restore --list` и SHA-256.
4. Развернуть код и выполнить `alembic upgrade head`.
5. Выполнить новый Pilot A только если нет свежего совместимого Gate A.
6. `collection campaign plan --apply`, затем применить Gate A к первому run через
   `collection capacity-apply --run-id ... --source ... --backup ...`.
7. Проверить stub metadata NULL, bulk links, checkpoint, отсутствие новых deadlock,
   coverage и переход между cohort без нового pilot.
8. После полного discovery проверить `metadata-preview`, затем маленький batch
   `groups.getById` и только после проверки продолжить metadata phase.

Стоп-условия: диск >=85%, новый deadlock, вторая active campaign, потеря checkpoint,
ранний metadata run, рост failed, несовпадение configuration/report/backup, дубли
или превышение памяти. Массовые посты subscription communities остаются выключены;
они требуют отдельного Pilot B, Gate B и прямого разрешения production rollout.

## Известные ограничения

- Production backlog из этой рабочей копии не вычислялся: доступа к production БД
  нет. Известный срез 2026-08-14 остаётся исходной оценкой до запуска read-only CLI.
- Без реального Pilot A нельзя заявить production throughput или точный ETA.
- Batch 100 для `groups.getById` выбран консервативно и должен уменьшаться при
  изменении официального контракта; увеличивать его без проверки нельзя.

## Проверенный локальный backlog и benchmark

Read-only `collection backlog --json` на текущей локальной PostgreSQL-копии
2026-08-14T20:10:57Z показал (это не повторный production-срез):

- approved groups: 17 336; без state/metadata — 5 076; без успешных posts — 7 857;
  без успешных members — 17 306;
- users: 12 285; доступных — 8 824; deactivated — 51; closed без доступа — 3 429;
- subscription states/links/communities в этой локальной копии — 0;
- pending `collect_group_members`: 29 596 rows, но 17 336 distinct entities —
  12 260 исторических повторов;
- pending `collect_group_posts`: 20 119 rows, но 17 336 distinct entities —
  2 783 исторических повтора;
- pending `refresh_group`: 17 336 rows и 17 336 distinct entities;
- stale running lease и active campaign — 0.

Именно state gaps, а не эти job rows, являются backlog. Для production необходимо
выполнить ту же read-only команду на production БД; известные цифры среза
2026-08-14 не выдаются за заново проверенные.

PostgreSQL benchmark использовал 500 fake users × 50 подписок с полностью
пересекающимися communities и обратным порядком ID для половины пользователей.
Он завершился за 25,58 с в ограниченном Docker-профиле, без deadlock и дублей,
создав 25 000 связей. Регрессия проверяет не более 10 storage statements на страницу;
теоретический старый row-by-row baseline — 100 group/link UPSERT на пользователя,
то есть не менее чем десятикратное сокращение числа storage statements.
