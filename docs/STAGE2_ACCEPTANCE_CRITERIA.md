# Критерии приёмки второго этапа

- [ ] Модели и явные constraints соответствуют документированной схеме; все даты UTC.
- [ ] Upgrade работает на текущей и чистой БД, повторный upgrade безопасен, check чист.
- [ ] Только approved и не-test группы получают jobs; планирование идемпотентно.
- [ ] `SKIP LOCKED`, lease expiration, heartbeat, pause/resume и retry проверены.
- [ ] Checkpoint сохраняется с каждой страницей; повторный run не создаёт дубли.
- [ ] Group metadata, posts, attachments, members, profiles и subscriptions реализованы.
- [ ] Private/closed/deleted корректно завершаются `skipped`, без scraping.
- [ ] Membership snapshot не деактивирует связи после неполного прохода.
- [ ] Не сохраняются contacts, binary media, raw VK JSON или tokens.
- [ ] Disk 85/95 gates и изоляция сбоя Telegram проверены.
- [ ] Privacy inspect/delete транзакционны и покрыты rollback-тестом.
- [ ] CLI plan/run/status/pause/resume/retry/verify/summary/pilot работает на русском.
- [ ] Backup создаётся и читается `pg_restore --list`.
- [ ] Unit, PostgreSQL integration и fake end-to-end smoke проходят без реальных tokens.
- [ ] CI выполняет Ruff, format, mypy, tests, compose/build, migrations и secret scan.
- [ ] Pilot по seed `20260728` завершён, результаты и реальный capacity estimate сохранены.
- [ ] Full run разрешён только при прогнозе <= 7 GiB и выполненных safety checks.
- [ ] Каждая разрешённая волна прошла verify; отчёт честно отражает paused/unsupported.
