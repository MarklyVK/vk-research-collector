# Оркестратор разработки VK Research Collector

Ты — главный инженер и оркестратор репозитория:

`MarklyVK/vk-research-collector`

Работай непосредственно с файлами проекта. Создавай код, запускай тесты и исправляй ошибки. Не ограничивайся рекомендациями.

## Сначала прочитай

- `AGENTS.md`
- `docs/PROJECT_CONTEXT.md`
- `docs/ACCEPTANCE_CRITERIA.md`
- `config/keywords.yml`

Затем проверь:

```bash
git status
git branch --show-current
git log --oneline -10

Не уничтожай существующие изменения.

Цель текущей итерации

Реализовать:

Поиск сообществ VK по ключевым словам.
Полную пагинацию результатов.
Сохранение уникальных групп в PostgreSQL.
Сохранение всех ключевых слов, по которым найдена группа.
Повторный поиск без дублей.
Экспорт pending-групп в JSON.
Импорт ручной классификации.
Итоговую статистику.
Docker, миграции, тесты и документацию.

Не реализовывать сбор постов, подписчиков и пользователей.

Начальный порядок действий
Создай docs/IMPLEMENTATION_PLAN.md.
Создай docs/DECISIONS.md.
Создай docs/PROGRESS.md.
Создай структуру Python-проекта.
Создай checkpoint commit.
После этого начинай реализацию.
Разделение работы

Если доступен запуск параллельных агентов, раздели работу:

Агент A — база данных

Владение:

src/database/**
alembic/**
модели и миграции

Задачи:

SQLAlchemy-модели;
Alembic;
ограничения уникальности;
индексы;
транзакции;
таблицы заданий и восстановления.
Агент B — VK API и поиск

Владение:

src/vk/**
src/search/**

Задачи:

асинхронный VK-клиент;
пул токенов;
rate limiting;
cooldown;
переключение токенов;
пагинация;
повторный поиск;
восстановление после перезапуска.
Агент C — классификация и CLI

Владение:

src/classification/**
src/cli/**

Задачи:

экспорт JSON;
импорт JSON;
проверка batch;
summary;
CLI-команды.
Агент D — инфраструктура

Владение:

Dockerfile
compose.yaml
.github/workflows/**
scripts/**
Makefile
README

Задачи:

Docker;
PostgreSQL;
CI;
деплой;
Telegram;
настройка Debian 12.
Агент E — тесты

Владение:

tests/**

Задачи:

unit-тесты;
integration-тесты;
fake VK transport;
Docker smoke test;
проверка секретов.

Если автоматический запуск подагентов недоступен, выполни эти роли самостоятельно по очереди.

Минимальная структура БД

Нужны сущности:

group_candidates;
search_keywords;
search_runs;
group_keyword_matches;
classification_batches;
classification_batch_items;
group_labels;
collection_jobs.

Группа уникальна по vk_id.

Обязательные CLI-команды
groups search
groups summary
classification export
classification import <file>
classification summary
collection start

Полные команды:

docker compose run --rm collector groups search
docker compose run --rm collector groups summary
docker compose run --rm collector classification export
docker compose run --rm collector classification import <file>
docker compose run --rm collector classification summary
docker compose run --rm collector collection start

Команда collection start пока должна только показать одобренные группы и сообщить, что основной сбор будет реализован позже.

Политика ошибок VK
Ошибка авторизации: отключить конкретный токен.
Rate limit: поставить токен на cooldown и выбрать другой.
Flood control: отложить запрос.
Timeout или серверная ошибка: повторить запрос.
Неверные параметры: не повторять бесконечно.
Все токены недоступны: поставить задания на паузу без потери прогресса.

Последовательность повторов:

1 минута;
5 минут;
15 минут;
1 час;
6 часов.

В тестах реальные ожидания запрещены.

Интеграция

Каждый подагент должен:

Работать в своей ветке или worktree.
Запустить свои тесты.
Сделать осмысленный commit.
Не выполнять merge и push.
Передать краткий отчёт.

Главный агент должен проверить diff и интегрировать изменения.

Финальная проверка

Выполни:

ruff check .
ruff format --check .
mypy src
pytest -q
docker compose config
docker compose build
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector pytest -q

Затем выполни fake VK smoke test:

Первый поиск.
Повторный поиск.
Проверка отсутствия дублей.
Проверка нескольких ключевых слов одной группы.
Экспорт batch.
Импорт одобренных ID.
Проверка статусов и меток.
Проверка восстановления после перезапуска.
Проверка отсутствия секретов в логах.
Финальный отчёт

Покажи:

что реализовано;
структуру проекта;
команды запуска на Windows;
команды запуска на Debian 12;
список GitHub Secrets;
список GitHub Variables;
результаты тестов;
известные ограничения;
какие значения должен добавить владелец.