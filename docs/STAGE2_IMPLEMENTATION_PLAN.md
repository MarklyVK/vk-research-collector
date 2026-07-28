# План реализации второго этапа

1. Зафиксировать требования, архитектуру, модель и предварительный capacity budget.
2. Сделать backup, миграцию stage 2 и проверить upgrade на текущей и чистой БД.
3. Безопасно исключить подтверждённые test fixtures; перевести integration-тесты на
   изолированную БД/транзакционную уборку.
4. Реализовать VK DTO/методы, нормализацию, PostgreSQL queue, lease и checkpoints.
5. Реализовать planners/workers для groups, posts, members, users и subscriptions.
6. Добавить CLI, privacy, disk gate, уведомления и наблюдаемость.
7. Добавить fake transport, unit/integration/smoke coverage и обновить CI/Docker.
8. Выполнить обязательные проверки; затем backup и реальный pilot.
9. Рассчитать capacity по фактическим rows/bytes и разрешить либо приостановить full run.
10. После каждой разрешённой волны выполнить verify, backup и обновить отчёт.
