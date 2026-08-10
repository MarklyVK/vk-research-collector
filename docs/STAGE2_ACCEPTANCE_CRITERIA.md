# Критерии приёмки второго этапа

- [x] Модели и явные constraints соответствуют документированной схеме; все даты UTC.
- [x] Upgrade работает на текущей и чистой БД, повторный upgrade безопасен, check чист.
- [x] Только approved и не-test группы получают jobs; планирование идемпотентно.
- [x] `SKIP LOCKED`, lease expiration, heartbeat, pause/resume и retry проверены.
- [x] Checkpoint сохраняется с каждой страницей; повторный run не создаёт дубли.
- [x] Group metadata, posts, attachments, members, profiles и subscriptions реализованы.
- [x] Private/closed/deleted корректно завершаются `skipped`, без scraping.
- [x] Membership snapshot не деактивирует связи после неполного прохода.
- [x] Не сохраняются contacts, binary media, raw VK JSON или tokens.
- [x] Disk 85/95 gates и изоляция сбоя Telegram проверены.
- [x] Privacy inspect/delete транзакционны и покрыты rollback-тестом.
- [x] CLI plan/run/status/pause/resume/retry/verify/summary/pilot работает на русском.
- [x] Backup создаётся и читается `pg_restore --list`.
- [x] Unit, PostgreSQL integration и fake end-to-end smoke проходят без реальных tokens.
- [x] CI workflow содержит Ruff, format, mypy, tests, compose/build, migrations и secret scan.
- [x] Pilot по seed `20260728` завершён, результаты и реальный capacity estimate сохранены.
- [x] Full run разрешён только при прогнозе <= 7 GiB и выполненных safety checks.
- [x] Уменьшенный repilot 100/200 прошёл gate: прогноз 3,89 GiB <= 7 GiB.
- [x] Full run разрешён только для groups/posts/members/users; subscriptions выключены.
- [x] Автономный `collector-worker` имеет `restart: unless-stopped`, пишет UTC progress
  logs и продолжил тот же run после реальной остановки/перезапуска контейнера.

## Расширение food_service

- [x] Четвёртая область добавлена в config, Pydantic, PostgreSQL CHECK, CLI и planner.
- [x] Migration 0005 проверена на чистой БД, восстановленной копии и рабочей БД.
- [x] Reclassification требует полный snapshot и сохраняет прежние labels.
- [x] Отдельный search поддерживает `--subject food_service`, known/new dedupe и counters.
- [x] Incremental planner не меняет основной run и требует audit/capacity gates.
- [ ] Завершены 37 407 семантических решений reclassification.
- [ ] Выполнены поиск, классификация новых групп и независимый аудит.
- [ ] Импортированы решения и создан отдельный incremental run.

## Endpoint-aware подписки

- [x] Коды 9 и 29 блокируют только точную пару token+method; другие методы токена работают.
- [x] Код 6 создаёт настраиваемый короткий global cooldown, auth errors отключают токен.
- [x] Method state не содержит секрет и восстанавливается из PostgreSQL после restart.
- [ ] Fair scheduler не допускает starvation и не claim-ит поток jobs недоступного метода.
- [x] Method deferral не расходует `max_attempts`; run автоматически выходит из
  `waiting_method_limit` после `next_wakeup_at`.
- [ ] Direct planner идемпотентно создаёт subscription jobs существующим доступным users.
- [x] `groups.get extended=1` сохраняет до 50 или 100 объектов и публичные метаданные.
- [ ] Private subscriptions (260) и недоступные сущности (15/18/30) terminal skipped.
- [x] `vk_communities` не создаёт и не меняет `group_candidates`; связи не дублируются.
- [x] Усечённый subscription snapshot не деактивирует прежние связи.
- [x] На уникальную community создаётся одна job и сохраняется не более 20 постов.
- [x] Candidate и subscription community используют один канонический post без дублей.
- [x] Миграция существующих subscription rows и около 700 тысяч posts выполняется порциями.
- [ ] Gate A (users/links/metadata) и Gate B (posts/attachments/indexes) дают JSON reports.
- [ ] Новые массовые scopes остаются выключены до backup, pilot и успешного capacity gate.
