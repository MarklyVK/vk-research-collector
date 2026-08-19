# Контекст проекта VK Research Collector

## 1. Общая информация

- GitHub Organization: `MarklyVK`
- Repository: `vk-research-collector`
- Репозиторий: приватный
- Среда разработки: Codex App на Windows
- Сервер: Cloud.ru
- Операционная система сервера: Debian 12
- GPU: отсутствует
- Все даты хранятся в UTC
- Production является ресурсно ограниченной средой; актуальные CPU, RAM, диск и swap
  необходимо проверять по live production report перед каждым rollout, а не брать из
  исторического описания.

## 2. Завершённый первый этап

Первый этап проекта завершён. В него входили:

1. Поиск сообществ VK по ключевым словам.
2. Сохранение найденных кандидатов в PostgreSQL.
3. Экспорт кандидатов в JSON.
4. Ручная классификация групп через отдельный чат ChatGPT.
5. Импорт списка одобренных VK ID.
6. Просмотр статистики.

С 30.07.2026 поддерживаются четыре области: `food_delivery`,
`customer_acquisition`, `tender_support`, `food_service` («Общепит»). Расширение
выполняется двумя независимыми потоками: полная повторная проверка 37 407 сохранённых
групп и отдельный поиск только по ключевым словам `food_service`.

Основной collection run `9be2813e-e1de-4ac9-bc07-7d92ac82438c` является snapshot
старого approved-набора. Новые approved-группы могут попасть только в отдельный
incremental run после семантической классификации, независимого аудита и capacity gate.

Общая последовательность:

```text
Ключевые слова
→ поиск через VK API
→ кандидаты
→ экспорт JSON
→ ручная классификация
→ импорт одобренных VK ID
→ одобренный набор сообществ
```

## 3. Цель второго этапа

Текущий этап — безопасный фазовый subscription enrichment уже сохранённых доступных
пользователей VK. Для каждой кампании материализуется неизменяемый snapshot; повторное
планирование не должно расширять или незаметно менять его. Текущий конфигурационный
лимит составляет не более 50 сохраняемых подписок на пользователя.

Основная последовательность:

```text
existing accessible users
→ immutable snapshot
→ aggregate subscription discovery capacity gate
→ subscription discovery
→ устранение unresolved пользователей
→ DISTINCT communities
→ aggregate metadata capacity gate
→ bounded metadata cohorts
```

Aggregate gate оценивает весь разрешённый snapshot и не может быть обойдён уменьшением
cohort. Rejected decision не создаёт campaign, snapshot, run или jobs. Перед следующими
cohorts выполняется live capacity recheck.

Небольшой light repair по уже сохранённым пользователям и сообществам может
чередоваться с bounded discovery cohorts. Metadata найденных сообществ, включая
название и описание, не начинается, пока discovery всего snapshot не завершён и
остаются unresolved пользователи. Для metadata применяется отдельный aggregate gate.

Scheduled production workflow работает только в report-only режиме. Mutating запуск
доступен лишь через ручной workflow_dispatch с точным confirmation. Subscription posts
и массовый `wall.get` выключены и не входят во второй этап.

## Решение владельца от 20.08.2026: личные стены пользователей

Владелец отдельно разрешил массовый `wall.get` только для личных стен уже сохранённых
доступных пользователей из approved-групп: не более 20 последних постов на пользователя
и не старше 180 дней. Это реализуется независимой durable campaign
`user_posts_enrichment` с фазой `user_posts_collection`, собственным immutable snapshot,
измеряемым pilot максимум на 500 пользователей, aggregate capacity gate всего snapshot
и live recheck перед следующими cohorts.

Разрешение не распространяется на стены сообществ из подписок, новых участников,
расширение snapshot, LLM API, векторизацию или кластеризацию. Jobs личных стен создаются
непосредственно из snapshot; `refresh_user_profile` их больше не создаёт.
