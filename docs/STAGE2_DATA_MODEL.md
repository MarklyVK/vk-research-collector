# Модель данных второго этапа

Все даты — `timestamptz` в UTC. Таблицы не хранят сырые ответы или токены. Основные
данные не очищаются автоматически; удаление пользователя — отдельная транзакционная
privacy-операция.

Endpoint-aware расширение добавляет `vk_token_states` и `vk_token_method_states`
(только fingerprint и timestamps), канонический реестр `vk_communities` и
`user_subscription_states`. `user_group_subscriptions.vk_group_id` получает FK на
`vk_communities.vk_id`. `group_posts.community_vk_id` получает FK на тот же реестр,
а прежний `group_id` становится nullable с `ON DELETE SET NULL`. Backfill выполняется
SQL-порциями и проверяет NULL/orphan rows до усиления constraints.

| Таблица | Назначение и ключи | Основные поля и индексы | Обновление / ожидаемый объём |
|---|---|---|---|
| `collection_runs` | Один план/запуск, PK UUID | scope, status, configuration JSONB, counters, timestamps; index status+created | Счётчики агрегируются по jobs; десятки/сотни строк |
| `collection_jobs` | PostgreSQL queue, PK UUID; FK run CASCADE; UQ run+type+entity | status, priority, attempt, next attempt, lease, checkpoint JSONB, metrics; partial queue/lease indexes | Одна строка на entity/scope; десятки тысяч |
| `group_collection_states` | PK/FK group | post/member checkpoints, success/error/skip, next run | Upsert; около 12 260 |
| `group_posts` | PK bigint; FK group CASCADE; UQ `(vk_owner_id,vk_post_id)` | published/edited, text, counters, signer, hash; indexes group+date, signer | Upsert одной версии; максимум 1,226 млн при лимите 100 |
| `post_attachments` | PK bigint; FK post CASCADE; UQ post+position | type, VK IDs, допустимый access key, размеры, duration, title, URL, малый JSONB metadata | Полная замена списка в page transaction; без binary/raw JSON |
| `vk_users` | PK = публичный VK ID | имя, фамилия, screen name, закрытость/deactivated, first/last seen, refreshed at | Публичный минимальный профиль; потенциально миллионы |
| `group_memberships` | PK bigint; FK group/user; UQ group+user | first/last seen, current, snapshot/source run; indexes user и group/current | Upsert; деактивация лишь после полного snapshot |
| `user_group_subscriptions` | PK bigint; FK user; UQ user+VK group ID | first/last seen, current, snapshot/source run | По умолчанию не планируется; отдельный gate |
| `collection_job_errors` | PK bigint; FK run/job CASCADE | token fingerprint, endpoint, category, VK/HTTP code, attempt, sanitized message, time | Append-only диагностическая история с retention |
| `search_run_groups` | UQ run+group | `was_new`, first seen | Дедуплицированная статистика known/new для отдельного search run |
| `classification_reviews` | UQ operation+group | previous/final approved и labels, confidence, reason, source | Неизменяемый аудит reclassification; прежние labels не удаляются |

`group_labels.label` и `search_keywords.subject` являются строками под PostgreSQL
CHECK, а не PostgreSQL enum. Migration `20260730_0005` расширяет оба ограничения
значением `food_service`, добавляет search counters и две аудит-таблицы. Downgrade
останавливается, если `food_service` уже используется, чтобы не потерять данные.

## Минимизация профиля

Сохраняются только `vk_id`, имя/фамилия для идентификации и дедупликации, публичный
`screen_name`, флаги доступности и deactivation. Пол, дата рождения, город и страна в
первой безопасной реализации не запрашиваются: для связного анализа они не нужны и
увеличивают privacy-риск. Контакты, адреса, взгляды, родственники и фотографии
запрещены.

## Ограничения и очистка

Явно именованные FK/UQ/check constraints обеспечивают дедупликацию. Run можно удалить
вместе с jobs/errors, но основные posts/users/links сохраняются; `source_run_id` при этом
становится NULL. `privacy delete-user` удаляет пользователя и каскадно membership и
subscription links. Group inspect не удаляет данные; удаление реальной группы требует
отдельного осознанного процесса и backup.
