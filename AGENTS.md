# AGENTS.md

## Перед началом работы

Обязательно прочитай:

1. `docs/PROJECT_CONTEXT.md`
2. `docs/ACCEPTANCE_CRITERIA.md`
3. `config/keywords.yml`
4. существующий код и Git-историю

Эти файлы являются источником требований.

## Текущий этап

Первый этап завершён. Его результат включает поиск сообществ VK по ключевым словам,
сохранение кандидатов и всех совпавших ключевых слов в PostgreSQL, повторный поиск без
дублей, экспорт кандидатов в JSON, импорт ручной классификации и статистику.

Сейчас реализуется второй этап — безопасный фазовый subscription enrichment:

- фазовый сбор не более настроенного лимита подписок уже сохранённых доступных
  пользователей VK (сейчас не более 50 на пользователя);
- immutable snapshot пользователей для каждой кампании;
- durable jobs, retries, checkpoints и состояния кампаний в PostgreSQL;
- aggregate capacity gate для всего snapshot до materialization и live capacity recheck
  перед следующими cohorts; gate нельзя обходить уменьшением cohort;
- сбор metadata найденных сообществ только после завершения subscription discovery для
  всего разрешённого snapshot и устранения unresolved пользователей;
- отдельный aggregate capacity gate перед metadata и bounded metadata cohorts;
- небольшие light-repair задачи по уже сохранённым пользователям и сообществам;
- безопасная карантинизация несовместимых legacy runs без удаления jobs и checkpoints;
- миграции, тесты, документация и production rollout защитного кода.

Scheduled production workflow работает только в режиме report-only. Любое mutating
действие требует явного apply или точного confirmation в ручном workflow_dispatch.

Без отдельного решения владельца не реализовывать и не запускать:

- сбор подписчиков или участников новых сообществ как новую массовую задачу;
- сбор всех постов, subscription posts и массовый `wall.get`;
- расширение snapshot за пределы критериев текущей кампании;
- запуск сбора при rejected capacity decision;
- автоматическое использование LLM API;
- векторизацию и кластеризацию;
- REST API и веб-интерфейс;
- удаление production jobs, checkpoints, runs или собранных данных;
- ручные SQL `UPDATE`/`DELETE` для обхода управляющей логики.

## Технологии

Используй:

- Python 3.12;
- PostgreSQL;
- SQLAlchemy 2.x;
- asyncpg;
- Alembic;
- httpx;
- Pydantic 2;
- Typer;
- PyYAML;
- pytest;
- Ruff;
- mypy;
- Docker Compose.

Не добавляй Redis, Celery, Kafka, RabbitMQ и Kubernetes.

## Правила разработки

- Документация и сообщения CLI — на русском.
- Названия классов, функций и переменных — на английском.
- Все даты хранятся в UTC.
- Публичные функции должны иметь type hints.
- Уникальность должна обеспечиваться PostgreSQL.
- Сырые ответы VK не сохраняются.
- Токены не сохраняются в БД и не выводятся в логах.
- Тесты не используют реальные VK-токены.
- Тесты не должны ждать реальные минуты.
- Код должен работать на сервере с 1 CPU и 1 GB RAM.

## Git

- Не уничтожай незакоммиченные изменения.
- Не используй `git reset --hard`.
- Не выполняй `git push` без прямого указания.
- Перед крупным изменением делай checkpoint commit.
- Если используются подагенты, каждый работает в отдельной ветке или worktree.
- Главный агент проверяет diff перед интеграцией.

## Обязательная проверка

Перед завершением выполни:

```bash
ruff check .
ruff format --check .
mypy src
pytest -q
docker compose config
docker compose build
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector pytest -q
```

Не заявляй полную готовность, если проверки не пройдены.
