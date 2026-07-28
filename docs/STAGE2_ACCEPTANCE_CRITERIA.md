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
- [x] Разрешённых full-волн нет; отчёт честно фиксирует `paused_capacity_limit`.
