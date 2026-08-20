# Критерии приёмки

## Исторические критерии завершённого первого этапа

1. Повторный поиск не создаёт дубли групп и связей.
2. Для группы сохраняются все совпавшие ключевые слова и предметные области.
3. Незавершённый поиск продолжается с сохранённой страницы после перезапуска.
4. Ошибка одного VK-токена не останавливает остальные токены.
5. Экспорт создаёт неизменяемые пакеты только из `pending`-кандидатов.
6. Импорт принимает сокращённый и полный JSON-формат.
7. Любая ошибка импорта полностью откатывает транзакцию.
8. На первом этапе `collection start` являлся безопасной заглушкой и не собирал данные;
   во втором этапе этот исторический критерий заменён управляемым фазовым запуском.
9. CI не требует реальных VK-токенов и не выполняет реальные ожидания.
10. Docker smoke test выполняет миграции и тесты с PostgreSQL.
11. Секреты отсутствуют в Git, образе и логах.
12. Документация содержит установку и эксплуатацию на Debian 12.

## Критерии второго этапа

1. Snapshot пользователей каждой кампании неизменяем после materialization.
2. Повторное планирование не создаёт duplicate campaign, run или job.
3. До materialization проверяется aggregate capacity всего разрешённого snapshot;
   уменьшение cohort не может обходить этот gate.
4. Rejected capacity gate не создаёт campaign, snapshot, run или jobs.
5. Перед следующими cohorts выполняется live capacity recheck.
6. Все discovery jobs завершаются либо получают явное terminal/resolved состояние.
7. Metadata jobs отсутствуют, пока discovery имеет unresolved пользователей.
8. Metadata требует отдельного aggregate capacity gate и выполняется bounded cohorts.
9. Несовместимые legacy runs сохраняются и безопасно переводятся в карантин без
   удаления jobs или checkpoints.
10. Scheduled production workflow работает только в report-only режиме.
11. Mutating start требует ручного workflow_dispatch и точного confirmation.
12. Subscription posts и массовый `wall.get` остаются выключенными.
13. Тесты не используют реальные VK-токены и не ждут реальные минуты.
14. Deployment требует валидный backup, Alembic upgrade, exact image SHA и успешные
    health checks PostgreSQL и worker.
15. `user_posts_enrichment` использует отдельный immutable snapshot с тем же eligible
    predicate и напрямую создаёт `collect_user_posts`, не используя profile-refresh.
16. User-post pilot ограничен 500 пользователями; production gate учитывает posts,
    attachments, state, snapshot, jobs и indexes с reserve factor не менее 1.30.
17. Rejected user-post gate не создаёт campaign/snapshot/run/jobs; уменьшение cohort не
    обходит решение, а перед следующей cohort выполняется live capacity recheck.
18. Owner-authorized bounded mode материализует только заранее объявленный лимит due users
    (150 000 subscriptions и 250 000 user posts), выполняет aggregate gate для всего этого
    snapshot и не расширяет его после создания.
19. Bounded mode сохраняет не менее 2 GiB свободного диска; абсолютный резерв действует
    одновременно с процентными warning/stop и проверяется worker перед выдачей jobs.
20. Замена старых capacity-rejected campaigns не удаляет jobs, checkpoints или собранные
    данные; terminal status получают только управляющие campaign/run и stale leases.
18. Личная стена сохраняет максимум 20 постов не старше 180 дней, включая корректный
    zero-post success, durable checkpoint/retry и terminal privacy/unavailable state.
19. `groups.get`, `users.get` и `wall.get` имеют независимые method cooldown; новый
    token fingerprint может восстановить `paused_no_tokens`, auth-disabled fingerprint
    автоматически не включается повторно.
20. Legacy quarantine применим только к incompatible/obsolete pilots с точным
    confirmation и не удаляет jobs, checkpoints или собранные данные.
21. `start-user-posts` требует ручного workflow_dispatch и точного `START_USER_POSTS`;
    scheduled workflow остаётся report-only.
