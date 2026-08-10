# VK Research Collector

Инструмент командной строки для воспроизводимого поиска сообществ VK, сохранения
кандидатов в PostgreSQL, ручной multi-label-классификации и безопасного
возобновляемого сбора публичных данных только по одобренным группам.

Проект рассчитан на маломощный сервер: Debian 12, 1 CPU, 1 GB RAM, 10 GB диска и
1 GB swap. PostgreSQL является единственным источником истины для данных, очередей,
checkpoint и истории операций. Все даты хранятся в UTC.

> **Состояние проекта.** Поиск, дедупликация, экспорт/импорт классификации, статистика,
> миграции, Docker и контур сбора approved-групп реализованы. Расширение предметной
> области `food_service` технически подготовлено, но его операционный workflow ещё не
> завершён: предстоят полная семантическая проверка сохранённых групп, отдельный поиск,
> классификация новых кандидатов, независимый аудит и отдельный incremental run.

## Содержание

- [Возможности](#возможности)
- [Границы проекта](#границы-проекта)
- [Предметные области](#предметные-области)
- [Как устроен проект](#как-устроен-проект)
- [Требования](#требования)
- [Быстрый запуск через Docker Compose](#быстрый-запуск-через-docker-compose)
- [Полный workflow поиска и классификации](#полный-workflow-поиска-и-классификации)
- [Расширение `food_service`](#расширение-food_service)
- [Возобновляемый сбор approved-данных](#возобновляемый-сбор-approved-данных)
- [Справочник CLI](#справочник-cli)
- [Переменные окружения](#переменные-окружения)
- [Модель данных](#модель-данных)
- [Надёжность и идемпотентность](#надёжность-и-идемпотентность)
- [Безопасность и приватность](#безопасность-и-приватность)
- [Разработка и тестирование](#разработка-и-тестирование)
- [Эксплуатация на Debian 12](#эксплуатация-на-debian-12)
- [Резервное копирование и восстановление](#резервное-копирование-и-восстановление)
- [CI/CD](#cicd)
- [Диагностика](#диагностика)
- [Структура репозитория](#структура-репозитория)
- [Дополнительная документация](#дополнительная-документация)

## Возможности

### Поиск сообществ

- поиск сообществ через официальный метод VK API `groups.search`;
- загрузка ключевых слов из `config/keywords.yml`;
- поиск сразу по всем областям или только по выбранной области через `--subject`;
- поддержка произвольного числа VK-токенов, по одному токену на строку;
- round-robin, индивидуальный rate limit и cooldown каждого токена;
- отключение только невалидного токена без остановки остальных;
- повторные запросы с backoff для временных ошибок API и сети;
- исключение закрытых и удалённых сообществ из списка кандидатов;
- checkpoint после каждой страницы до 1000 результатов;
- автоматическое продолжение незавершённого запуска с сохранённого offset;
- PostgreSQL-дедупликация групп, совпадений и групп внутри запуска;
- сохранение всех ключевых слов и областей, по которым встретилась группа;
- отдельные счётчики новых, уже известных, закрытых, удалённых групп и ошибок.

### Ручная классификация

- атомарный экспорт фиксированных JSON-пакетов из `pending`-кандидатов;
- защита состава экспортированного пакета от последующего изменения;
- сокращённый формат импорта только со списком approved VK ID;
- полный формат импорта с решением, метками и уверенностью по каждой группе;
- статусы `pending`, `approved`, `rejected`;
- multi-label-классификация по четырём предметным областям;
- полная валидация документа до изменения данных;
- транзакционный импорт: любая ошибка откатывает весь пакет;
- безопасный повтор идентичного импорта;
- статистика по статусам и меткам;
- отдельный workflow полной повторной классификации и независимого аудита.

### Сбор approved-данных

- предварительный план без изменения базы;
- детерминированный pilot и capacity gate перед full run;
- PostgreSQL-очередь с `FOR UPDATE SKIP LOCKED`, lease и heartbeat;
- foreground worker и автономный Docker-сервис `collector-worker`;
- возобновление с checkpoints после штатной остановки или сбоя;
- pause, resume и повтор failed-задач без сброса прогресса;
- сбор метаданных групп, постов, участников и минимальных профилей;
- опциональный сбор публичных подписок, выключенный по умолчанию;
- upsert и уникальные ограничения вместо хранения дублей;
- агрегированная диагностика, проверка инвариантов и журнал очищенных ошибок;
- отдельный incremental run, не изменяющий snapshot основного run.

### Инфраструктура

- Python 3.12, SQLAlchemy 2.x, asyncpg, Alembic, httpx, Pydantic 2 и Typer;
- PostgreSQL 16;
- Docker Compose для разработки, smoke test и production;
- отдельная read-only роль PostgreSQL;
- ограничение CPU, памяти и ротация Docker-логов;
- Ruff, mypy strict, pytest и PostgreSQL integration tests;
- GitHub Actions, GHCR и deployment на repository-scoped self-hosted runner;
- backup перед deployment, health/progress checks и rollback Docker image.

## Границы проекта

Проект работает только с данными, доступными через официальный VK API. Он не выполняет
HTML scraping и не скачивает бинарные медиафайлы.

В текущую архитектуру намеренно не входят:

- REST API и веб-интерфейс;
- Redis, Celery, Kafka, RabbitMQ и Kubernetes;
- автоматические вызовы LLM API;
- векторизация и кластеризация;
- сохранение сырых ответов VK;
- хранение токенов в PostgreSQL или вывод токенов в логах.

Ручная классификация выполняется вне приложения. Репозиторий содержит
`CLASSIFICATION_PROMPT.md`, но сам collector не отправляет данные в ChatGPT или другой
LLM.

## Предметные области

Источник конфигурации — [`config/keywords.yml`](config/keywords.yml). Порядок областей
фиксирован и валидируется при загрузке.

| Машинное имя | Русское название | Назначение | Ключевых слов |
|---|---|---|---:|
| `food_delivery` | Доставка еды | Доставка готовой еды и продуктов | 22 |
| `customer_acquisition` | Привлечение клиентов | Маркетинг, лидогенерация и продажи | 26 |
| `tender_support` | Тендеры и торги | Закупки и сопровождение торгов | 23 |
| `food_service` | Общепит | Заведения, непосредственно готовящие и продающие еду или напитки посетителям | 28 |

Всего настроено 99 уникальных ключевых слов. Загрузчик нормализует пробелы, регистр и
`ё/е` для обнаружения конфигурационных дублей. Одно и то же нормализованное ключевое
слово не может одновременно принадлежать разным областям.

`food_service` не заменяет `food_delivery`: ресторан с собственной доставкой может
получить обе метки.

Чтобы изменить поисковый словарь:

1. Отредактируйте `config/keywords.yml` в UTF-8.
2. Не меняйте четыре машинных имени и их порядок без миграции схемы и обновления типов.
3. Убедитесь, что список каждой области непустой и в нём нет нормализованных дублей.
4. Выполните `pytest -q tests/test_config.py` и полный набор проверок.
5. Перед production-поиском создайте backup.

## Как устроен проект

```mermaid
flowchart LR
    K["config/keywords.yml"] --> S["groups search"]
    T["VK tokens\nread-only file"] --> V["VK API client"]
    V --> S
    S --> P[("PostgreSQL")]
    P --> E["classification export"]
    E --> H["ручная разметка"]
    H --> I["classification import"]
    I --> P
    P --> Q["collection plan / pilot"]
    Q --> W["foreground или autonomous worker"]
    W --> V
    W --> P
```

Основные принципы:

- CLI не хранит долгоживущее состояние в памяти: прогресс записывается в PostgreSQL;
- каждая страница поиска или сбора завершается короткой транзакцией;
- сетевой запрос не выполняется внутри одной транзакции на весь scope;
- уникальность и защита от дублей обеспечиваются ограничениями PostgreSQL;
- секреты приходят только из переменных окружения и read-only token file;
- файлы экспорта создаются через временный файл и атомарное переименование;
- контейнер и база работают в UTC.

## Требования

Для рекомендуемого Docker-запуска нужны:

- Git;
- Docker Engine или Docker Desktop;
- Docker Compose plugin (`docker compose`);
- один или несколько действующих VK-токенов;
- свободный локальный порт 5432 либо другое значение `POSTGRES_PORT`.

Для запуска без Docker дополнительно нужны:

- Python 3.12;
- PostgreSQL 16 или совместимая поддерживаемая версия;
- системные инструменты PostgreSQL для backup/restore при эксплуатации.

Проверка окружения:

```bash
docker --version
docker compose version
git --version
```

## Быстрый запуск через Docker Compose

### 1. Подготовьте runtime-файлы

Linux/macOS/Git Bash:

```bash
cp .env.example .env
mkdir -p secrets exports backups
touch secrets/vk_tokens.txt
chmod 600 .env secrets/vk_tokens.txt
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
New-Item -ItemType Directory -Force secrets, exports, backups
New-Item -ItemType File -Force secrets/vk_tokens.txt
```

Откройте `.env` и обязательно замените:

```dotenv
POSTGRES_PASSWORD=случайный_стойкий_пароль
POSTGRES_READER_PASSWORD=другой_случайный_стойкий_пароль
POSTGRES_BIND_ADDRESS=127.0.0.1
```

В `secrets/vk_tokens.txt` поместите токены, по одному на строку:

```text
FIRST_VK_TOKEN
SECOND_VK_TOKEN
```

Пустые строки игнорируются. Не добавляйте комментарии: любая непустая строка считается
токеном.

### 2. Проверьте Compose и запустите PostgreSQL

```bash
docker compose config
docker compose up -d postgres
docker compose ps
```

По умолчанию PostgreSQL привязан к `127.0.0.1:5432`. Не меняйте адрес на `0.0.0.0`
без firewall и осознанной необходимости.

### 3. Примените миграции

```bash
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector alembic current
docker compose run --rm collector alembic check
```

### 4. Проверьте CLI

```bash
docker compose run --rm collector --help
docker compose run --rm collector groups --help
docker compose run --rm collector classification --help
```

### 5. Выполните первый поиск

```bash
docker compose run --rm collector groups search
docker compose run --rm collector groups summary
```

То же через Make:

```bash
make up
make migrate
make search-groups
make groups-summary
```

## Полный workflow поиска и классификации

### Шаг 1. Поиск кандидатов

Все области:

```bash
docker compose run --rm collector groups search
```

Только одна область:

```bash
docker compose run --rm collector groups search --subject food_service
```

Разрешённые значения `--subject`:

```text
food_delivery
customer_acquisition
tender_support
food_service
```

На старте вычисляется `plan_key` из упорядоченного списка ключевых слов. Если для того
же плана существует последний `running` или `paused` run, команда продолжит его. Для
каждой пары «ключевое слово + тип сообщества» хранится `next_offset`. После успешного
сохранения страницы offset фиксируется в той же транзакции.

Пример итоговой статистики запуска:

```json
{
  "run_id": "00000000-0000-0000-0000-000000000000",
  "status": "completed",
  "subjects": ["food_delivery", "customer_acquisition"],
  "total_api_results": 12000,
  "unique_vk_groups": 8700,
  "already_known_groups": 1200,
  "new_groups": 7500,
  "private_results": 140,
  "deleted_results": 17,
  "errors": 0
}
```

Ошибка конкретного ключевого слова переводит его checkpoint в `failed`, увеличивает
счётчик ошибок и не прерывает обработку остальных ключей. Если закончились все рабочие
токены, run получает `paused`; после восстановления token file повторная команда
продолжает этот же план.

Повторный полный поиск создаёт новую историю запуска, но не дублирует группы и пары
«группа + ключевое слово». Публичные поля группы и `last_matched_at` обновляются.

### Шаг 2. Статистика кандидатов

```bash
docker compose run --rm collector groups summary
```

Команда показывает:

- число уникальных групп;
- число уникальных совпадений с ключевыми словами;
- число запусков поиска.

### Шаг 3. Экспорт пакета классификации

```bash
docker compose run --rm collector classification export
```

По умолчанию создаётся пакет до 100 групп в
`exports/classification/<batch-id>.json`. Размер задаётся
`CLASSIFICATION_BATCH_SIZE`, каталог внутри контейнера — `EXPORT_DIR`.

В пакет попадают только группы со статусом `pending`, которые ещё не включались в
предыдущие пакеты. Выборка блокируется через `FOR UPDATE SKIP LOCKED`, поэтому
параллельные экспортёры не должны получить одну и ту же группу.

Формат экспорта:

```json
{
  "batch_id": "2026-07-31-a1b2c3d4e5f6",
  "groups": [
    {
      "vk_id": 123456,
      "name": "Пример сообщества",
      "description": "Публичное описание",
      "status": "Статус сообщества",
      "address": "https://vk.com/example",
      "matched_keywords": [
        {
          "keyword": "доставка еды",
          "subject": "food_delivery"
        }
      ]
    }
  ]
}
```

Состав готового пакета является snapshot: элементы нельзя изменить или удалить.

### Шаг 4. Ручная классификация

Используйте [`CLASSIFICATION_PROMPT.md`](CLASSIFICATION_PROMPT.md) как основу для
отдельного процесса разметки. Collector не вызывает LLM автоматически.

Поддерживаются два импортных формата.

Сокращённый формат перечисляет только approved-группы:

```json
{
  "batch_id": "2026-07-31-a1b2c3d4e5f6",
  "approved_group_ids": [123456, 987654]
}
```

Все остальные группы пакета становятся `rejected`. Approved-группы получают метки из
предметных областей совпавших ключевых слов и `confidence=1.0`.

Полный формат содержит решение для каждой группы пакета:

```json
{
  "batch_id": "2026-07-31-a1b2c3d4e5f6",
  "results": [
    {
      "vk_id": 123456,
      "approved": true,
      "labels": ["food_delivery", "food_service"],
      "confidence": 0.96
    },
    {
      "vk_id": 222222,
      "approved": false,
      "labels": [],
      "confidence": 0.91
    }
  ]
}
```

Правила полного формата:

- документ должен содержать ровно один из ключей `approved_group_ids` или `results`;
- VK ID не повторяются;
- полный формат содержит решение для каждой группы пакета и не содержит чужих ID;
- `confidence` находится в диапазоне от 0 до 1;
- у approved-группы есть хотя бы одна уникальная разрешённая метка;
- у rejected-группы `labels` пуст;
- неизвестные поля запрещены.

### Шаг 5. Импорт классификации

Положите результат в каталог `exports`, смонтированный как `/app/exports`, затем:

```bash
docker compose run --rm collector classification import \
  /app/exports/classification/results.json
```

Через Make:

```bash
make import-classification FILE=/app/exports/classification/results.json
```

Импорт получает PostgreSQL advisory lock, блокирует пакет, проверяет весь документ,
обновляет статусы и заменяет метки в одной транзакции. Любая ошибка приводит к полному
rollback. Повторный импорт уже применённого пакета допустим только при точном совпадении
с сохранённым результатом; тогда изменяется 0 строк.

### Шаг 6. Статистика классификации

```bash
docker compose run --rm collector classification summary
docker compose run --rm collector classification summary --subject food_service
```

Общая статистика показывает `pending`, `approved`, `rejected` и распределение
approved-групп по меткам. Фильтр области дополнительно показывает число кандидатов,
статусы, multi-label-группы и уже собранные группы этой области.

## Расширение `food_service`

Этот workflow предназначен для полной повторной проверки уже сохранённых кандидатов и
для последующего отдельного поиска. Он не должен изменять основной collection snapshot
`9be2813e-e1de-4ac9-bc07-7d92ac82438c`.

### 1. Создайте и проверьте backup

```bash
make backup PURPOSE=before-food-service-reclassification
```

### 2. Примените актуальные миграции

```bash
docker compose run --rm collector alembic upgrade head
```

### 3. Подготовьте полный snapshot

```bash
docker compose run --rm collector classification reclassification-prepare
```

По умолчанию создаются:

```text
exports/food-service-reclassification/
├── decisions.json
├── progress.json
├── validation-summary.json
└── RECLASSIFICATION_REPORT.md
```

`decisions.json` содержит контекст каждой группы, прежний статус, прежние метки и
SHA-256 канонического snapshot. Для каждой группы требуется семантическое решение.
Reclassification может добавлять `food_service` и переводить релевантную rejected-группу
в approved, но не может удалять прежние корректные метки. Отрицательное решение не
должно менять прежнюю классификацию.

### 4. Проверьте заполненный документ без изменения БД

```bash
docker compose run --rm collector classification reclassification-validate \
  /app/exports/food-service-reclassification/decisions.json
```

Проверяется полнота, уникальность VK ID, неизменность snapshot, согласованность
`food_service`, `final_approved`, `final_labels`, confidence и причины решения.

### 5. Импортируйте только после нового backup

```bash
docker compose run --rm \
  --volume "$PWD/backups:/app/backups:ro" \
  collector classification reclassification-import \
  /app/exports/food-service-reclassification/decisions.json \
  --backup /app/backups/verified-before-import.dump
```

Путь `--backup` обязателен. CLI проверяет, что файл существует и не пуст; оператор
отвечает за то, что это действительно проверенный PostgreSQL backup, созданный
непосредственно перед импортом. Каталог `backups` не смонтирован в базовом
`compose.yaml`, поэтому пример добавляет read-only bind mount. Операция использует
advisory lock, блокировку групп, пакетные записи и неизменяемый журнал
`classification_reviews`. Повтор полностью применённого `operation_id` идемпотентен.

### 6. Выполните отдельный поиск и обычную классификацию новых кандидатов

```bash
docker compose run --rm collector groups search --subject food_service
docker compose run --rm collector classification export
docker compose run --rm collector classification import /app/exports/classification/results.json
docker compose run --rm collector classification summary --subject food_service
```

### 7. Подготовьте независимый аудит

```bash
docker compose run --rm collector classification audit-prepare \
  /app/exports/food-service-reclassification/decisions.json
```

Аудит использует фиксированный seed `20260730` и стратифицированную выборку до 500
групп. После независимой разметки:

```bash
docker compose run --rm collector classification audit-validate \
  /app/exports/food-service-audit/audit-results.json
```

Quality gates:

- precision не ниже 0.90;
- false-negative rate не выше 0.10;
- exact match multi-label не ниже 0.85;
- отсутствие систематической ошибки выше 0.10 в достаточно большой страте.

Incremental plan разрешён только при `decision=passed` в машиночитаемом audit summary.

## Возобновляемый сбор approved-данных

> Этот контур работает только с `approved`-группами. Перед full или incremental run
> изучите [`docs/STAGE2_OPERATIONS.md`](docs/STAGE2_OPERATIONS.md) и создайте backup.

### Типы собираемых данных

| Scope | Начальная задача | Результат |
|---|---|---|
| `groups` | `refresh_group` | актуальные публичные поля группы |
| `posts` | `collect_group_posts` | нормализованные посты и метаданные вложений |
| `members` | `collect_group_members` | участники и memberships |
| `users` | `refresh_user_profile` | минимальные публичные профили участников |
| `subscriptions` | `collect_user_subscriptions` | публичные подписки пользователей; выключено по умолчанию |

Задания пользователей создаются динамически при обработке участников. Полные профили
не запрашиваются повторно до истечения `COLLECTION_USER_PROFILE_TTL_DAYS`.

### Безопасный порядок запуска

```bash
docker compose up -d postgres
make backup PURPOSE=before-collection
make migrate
make collection-plan
make collection-pilot
make collection-status
```

`collection start` и `collection plan` без `--apply` ничего не меняют в базе.
Предпросмотр показывает число approved/selected групп, scopes, jobs, ожидаемые API
запросы и предупреждения.

### Pilot и capacity gate

```bash
docker compose run --rm collector collection pilot
```

Pilot:

1. детерминированно выбирает до `COLLECTION_PILOT_GROUPS_PER_CATEGORY` групп каждой
   категории с seed `COLLECTION_PILOT_SEED`;
2. создаёт и выполняет pilot run;
3. измеряет рост базы;
4. экстраполирует объём на весь approved-набор с запасом 30%;
5. записывает `pilot-summary.json` и `capacity-estimate.json`;
6. выдаёт `passed`, только если run завершён, прогноз вычислен и не превышает 7 GiB.

Full run не запускается, если его collection-конфигурация не совпадает с конфигурацией
успешного capacity report.

Создание full run:

```bash
docker compose run --rm collector collection plan --apply
docker compose run --rm collector collection status
```

Если требуется явно применить проверенный capacity report к созданному full run:

```bash
docker compose run --rm collector collection capacity-apply \
  --run-id RUN_ID \
  --source /app/exports/stage2-pilot/capacity-estimate.json
```

### Foreground worker

Ограниченный запуск:

```bash
docker compose run --rm collector collection run --run-id RUN_ID --max-jobs 10
```

Обработка до опустошения очереди:

```bash
docker compose run --rm collector collection run --run-id RUN_ID --until-idle
```

Только один scope:

```bash
docker compose run --rm collector collection run \
  --run-id RUN_ID --scope posts --until-idle
```

Разрешённые значения `--scope`: `groups`, `posts`, `members`, `users`,
`subscriptions`.

### Автономный worker

```bash
docker compose up -d collector-worker
docker compose ps
docker compose logs -f collector-worker
```

Сервис имеет `restart: unless-stopped`, находит последний разрешённый run и хранит весь
прогресс в PostgreSQL. При SIGTERM worker заканчивает текущую page transaction и не
захватывает новые jobs. После запуска он восстанавливает просроченные lease и продолжает
с checkpoint.

На Debian включите автозапуск Docker:

```bash
sudo systemctl enable --now docker
docker compose up -d postgres collector-worker
```

На Windows автоматическое продолжение после перезагрузки зависит от запуска Docker
Desktop.

### Управление run

```bash
docker compose run --rm collector collection status --run-id RUN_ID
docker compose run --rm collector collection pause --run-id RUN_ID
docker compose run --rm collector collection resume --run-id RUN_ID
docker compose run --rm collector collection retry-failed --run-id RUN_ID
docker compose run --rm collector collection verify --run-id RUN_ID
docker compose run --rm collector collection summary
```

`pause` переводит pending/retry jobs в `paused`. `resume` возвращает paused jobs в
очередь без сброса checkpoint. `retry-failed` возвращает failed jobs в pending, также
сохраняя прогресс.

`verify` проверяет:

- дубли постов;
- дубли memberships;
- дубли subscriptions;
- задания rejected-групп;
- просроченные locks;
- согласованность счётчиков run.

### Incremental run для новых `food_service`-групп

Основной run остаётся неизменяемым snapshot. После успешной reclassification,
классификации новых кандидатов и независимого аудита:

```bash
docker compose run --rm collector collection plan --apply \
  --incremental-from 9be2813e-e1de-4ac9-bc07-7d92ac82438c \
  --reason food_service_increment \
  --source food_service_expansion \
  --audit-summary /app/exports/food-service-audit/summary.json
```

Planner исключает группы snapshot базового run и не создаёт уже успешно завершённые для
них задачи. Если оценка диска достигает warning threshold, run создаётся в
`paused_capacity_limit` и worker его не выбирает.

## Справочник CLI

Общий формат запуска:

```bash
docker compose run --rm collector [ГРУППА] [КОМАНДА] [АРГУМЕНТЫ]
```

После editable install без Docker используется команда `collector`.

### `groups`

| Команда | Назначение | Основные параметры |
|---|---|---|
| `groups search` | Запустить или продолжить поиск | `--subject TEXT` |
| `groups summary` | Показать общую статистику поиска | — |

### `classification`

| Команда | Назначение | Основные параметры |
|---|---|---|
| `classification export` | Создать следующий immutable batch | — |
| `classification import SOURCE` | Транзакционно импортировать JSON | `SOURCE` — файл |
| `classification summary` | Показать статусы и метки | `--subject TEXT` |
| `classification reclassification-prepare` | Экспортировать полный snapshot | `--output-dir PATH` |
| `classification reclassification-validate SOURCE` | Проверить решения без записи | `SOURCE` — файл |
| `classification reclassification-import SOURCE` | Импортировать решения | обязательный `--backup FILE` |
| `classification audit-prepare DECISIONS` | Создать выборку аудита | `--output-dir PATH` |
| `classification audit-validate SOURCE` | Рассчитать quality gates | `--output PATH` |

### `collection`

| Команда | Назначение | Основные параметры |
|---|---|---|
| `collection start` | Совместимый безопасный preview | — |
| `collection plan` | Показать или применить план | `--apply`, `--pilot`, `--incremental-from UUID`, `--reason`, `--source`, `--audit-summary PATH` |
| `collection pilot` | Выполнить pilot и оценить capacity | — |
| `collection capacity-apply` | Применить успешный capacity report | `--run-id UUID`, `--source PATH`; для подписок также `--backup DUMP` |
| `collection run` | Запустить foreground worker | `--run-id UUID`, `--scope TEXT`, `--max-jobs N`, `--until-idle` |
| `collection worker` | Запустить автономный worker | — |
| `collection status` | Показать состояние run | `--run-id UUID` |
| `collection pause` | Приостановить run | `--run-id UUID` |
| `collection resume` | Продолжить paused run | `--run-id UUID` |
| `collection retry-failed` | Повторить failed jobs | `--run-id UUID` |
| `collection verify` | Проверить инварианты | `--run-id UUID` |
| `collection summary` | Показать глобальные количества | — |

### `privacy`

| Команда | Назначение | Основные параметры |
|---|---|---|
| `privacy inspect-user VK_ID` | Показать безопасные агрегаты пользователя | положительный VK ID |
| `privacy inspect-group VK_ID` | Показать безопасные агрегаты группы | положительный VK ID |
| `privacy delete-user VK_ID` | Транзакционно удалить пользователя и зависимости | обязательный `--confirm` |

Всегда создавайте и проверяйте backup перед `privacy delete-user`.

### Makefile

| Target | Эквивалент/назначение |
|---|---|
| `make up` | Запустить PostgreSQL |
| `make down` | Остановить Compose-проект без удаления volume |
| `make migrate` | `alembic upgrade head` |
| `make search-groups` | Поиск групп |
| `make groups-summary` | Статистика поиска |
| `make export-classification` | Экспорт batch |
| `make import-classification FILE=...` | Импорт классификации |
| `make classification-summary` | Статистика классификации |
| `make collection-plan [APPLY=1]` | Preview или создание full plan |
| `make collection-pilot` | Pilot |
| `make collection-capacity-apply RUN_ID=...` | Применить capacity report |
| `make collection-run [RUN_ID=...]` | Worker до idle |
| `make collection-status [RUN_ID=...]` | Статус |
| `make collection-pause RUN_ID=...` | Pause |
| `make collection-resume RUN_ID=...` | Resume |
| `make collection-retry-failed RUN_ID=...` | Повтор failed jobs |
| `make collection-verify RUN_ID=...` | Проверка run |
| `make collection-summary` | Глобальная статистика |
| `make backup PURPOSE=...` | Проверенный custom-format dump |
| `make test` | Тесты внутри collector image |
| `make lint` | Ruff, format check и mypy внутри image |
| `make smoke` | Полный deployment smoke test |
| `make logs` | Последние 200 строк и follow |

## Переменные окружения

Скопируйте `.env.example` в `.env`. Pydantic читает переменные окружения и локальный
`.env`; Docker Compose передаёт значения контейнеру явно.

### Приложение и VK API

| Переменная | По умолчанию | Описание |
|---|---|---|
| `APP_ENV` | `development` | Название окружения |
| `LOG_LEVEL` | `INFO` | Уровень логирования |
| `TZ` | `UTC` | Часовой пояс контейнеров; должен оставаться UTC |
| `VK_API_VERSION` | `5.199` | Версия VK API |
| `VK_TOKENS_FILE` | `/run/secrets/vk_tokens.txt` | Файл токенов внутри контейнера |
| `VK_REQUEST_TIMEOUT_SECONDS` | `30` | HTTP timeout одного запроса |
| `VK_MAX_CONCURRENCY` | `3` | Зарезервированная настройка; текущий VK client напрямую её не использует |
| `VK_PER_TOKEN_RPS` | `2.5` | Лимит запросов в секунду на один токен |

### Классификация

| Переменная | По умолчанию | Описание |
|---|---|---|
| `CLASSIFICATION_BATCH_SIZE` | `100` | Максимум групп в одном batch |
| `EXPORT_DIR` | `/app/exports/classification` | Каталог batch внутри контейнера |

### PostgreSQL

| Переменная | По умолчанию | Описание |
|---|---|---|
| `POSTGRES_DB` | `vk_research` | Имя базы |
| `POSTGRES_USER` | `vk_collector` | Роль приложения и миграций |
| `POSTGRES_PASSWORD` | `change_me` в примере | Пароль приложения; обязательно заменить |
| `POSTGRES_HOST` | `postgres` | Host базы внутри Compose |
| `POSTGRES_PORT` | `5432` | Порт контейнера/публикации |
| `POSTGRES_BIND_ADDRESS` | `127.0.0.1` в `.env.example` | Адрес публикации PostgreSQL на host |
| `POSTGRES_VOLUME_NAME` | `vk_research_postgres_data` | Стабильное имя production volume |
| `DATABASE_URL` | собирается из полей выше | Полный SQLAlchemy URL; имеет приоритет |
| `POSTGRES_READER_USER` | `vk_reader` | Read-only роль |
| `POSTGRES_READER_PASSWORD` | `change_reader_password` | Пароль read-only роли; обязательно заменить |

Пароль в автоматически собранном SQLAlchemy URL URL-encode-ится. URL и пароли не
должны попадать в логи.

### Telegram и диск

| Переменная | По умолчанию | Описание |
|---|---|---|
| `TELEGRAM_ENABLED` | `false` | Включить операционные уведомления |
| `TELEGRAM_BOT_TOKEN` | пусто | Устаревающий inline fallback; production setup очищает его |
| `TELEGRAM_BOT_TOKEN_FILE` | `secrets/telegram_bot_token.txt` | Отдельный runtime secret, owner `deploy`, mode `600` |
| `TELEGRAM_CHAT_ID` | пусто | Получатель уведомлений |
| `TELEGRAM_TIMEZONE` | `Europe/Moscow` | Зона ежедневной сводки |
| `TELEGRAM_ALERT_REPEAT_SECONDS` | `10800` | Повтор продолжающейся проблемы |
| `TELEGRAM_STALL_MINUTES` | `30` | Порог отсутствия collection progress |
| `TELEGRAM_DISK_WARNING_PERCENT` | `85` | Порог Telegram warning по диску |
| `TELEGRAM_DISK_CRITICAL_PERCENT` | `95` | Порог Telegram critical по диску |
| `TELEGRAM_RAM_WARNING_AVAILABLE_MB` | `100` | Минимум доступной RAM |
| `TELEGRAM_COLLECTION_RUN_ID` | пусто | Явный приоритетный run для monitor |
| `DISK_WARNING_PERCENT` | `85` | Порог предупреждения и запрета тяжёлых операций |
| `DISK_STOP_PERCENT` | `95` | Критический порог остановки |

Production monitor выполняет read-only проверки каждые пять минут, отправляет recovery
и ежедневную сводку ровно в `09:00 Europe/Moscow`. Token безопасно настраивается через
`sudo ./scripts/setup-telegram-monitor.sh`; полная эксплуатация и список проверок
описаны в [`docs/TELEGRAM_MONITORING.md`](docs/TELEGRAM_MONITORING.md).

### Worker и lease

| Переменная | По умолчанию | Описание |
|---|---|---|
| `COLLECTION_WORKER_ID` | `collector-1` | Уникальное имя worker |
| `COLLECTION_MAX_CONCURRENCY` | `3` | Максимальная конкурентность, допустимо 1–10 |
| `COLLECTION_JOB_LEASE_SECONDS` | `300` | Срок lease задания, минимум 30 секунд |
| `COLLECTION_JOB_HEARTBEAT_SECONDS` | `60` | Зарезервированный интервал heartbeat; lease recovery сейчас опирается на timestamps задания |
| `COLLECTION_IDLE_SLEEP_SECONDS` | `5` | Пауза автономного worker без доступных задач |
| `COLLECTION_RUN_ID` | пусто | Основной run для production-скриптов и handoff |

### Посты

| Переменная | По умолчанию | Описание |
|---|---|---|
| `COLLECTION_POSTS_ENABLED` | `true` | Включить scope постов |
| `COLLECTION_POSTS_MAX_PER_GROUP` | `100` | Максимум постов на группу |
| `COLLECTION_POSTS_PAGE_SIZE` | `100` | Размер страницы VK, максимум 100 |
| `COLLECTION_POSTS_INCLUDE_PINNED` | `true` | Зарезервированный флаг; текущий worker сохраняет возвращённые API закреплённые посты независимо от него |
| `COLLECTION_POSTS_STOP_AT_DATE` | пусто | Входит в plan/capacity configuration; остановка worker по дате пока не реализована |

### Участники и пользователи

| Переменная | По умолчанию | Описание |
|---|---|---|
| `COLLECTION_MEMBERS_ENABLED` | `true` | Включить scope участников |
| `COLLECTION_MEMBERS_MAX_PER_GROUP` | `200` | Максимум участников; пусто означает без явного лимита |
| `COLLECTION_MEMBERS_PAGE_SIZE` | `1000` | Размер страницы VK, максимум 1000 |
| `COLLECTION_USERS_ENABLED` | `true` | Создавать задачи обновления профилей |
| `COLLECTION_USER_PROFILE_TTL_DAYS` | `30` | Не обновлять свежий профиль до истечения TTL |
| `COLLECTION_USER_BATCH_SIZE` | `1000` | Размер batch для `users.get`, максимум 1000 |

### Подписки

| Переменная | По умолчанию | Описание |
|---|---|---|
| `COLLECTION_SUBSCRIPTIONS_ENABLED` | `false` | Включить публичные подписки; требует отдельного capacity gate |
| `COLLECTION_SUBSCRIPTIONS_MAX_PER_USER` | `50` | Максимум подписок пользователя, не более 100 |
| `COLLECTION_SUBSCRIPTIONS_PAGE_SIZE` | `50` | Размер страницы VK, не более 1000; запрос ограничивается остатком лимита 50/100 |
| `COLLECTION_SUBSCRIPTION_PILOT_USERS` | `500` | Максимальный размер Pilot A |
| `COLLECTION_SUBSCRIPTION_PILOT_MIN_USERS` | `100` | Минимальное наблюдение для разрешающего Gate A |
| `COLLECTION_SUBSCRIPTION_POSTS_PILOT_COMMUNITIES` | `500` | Максимальный размер Pilot B |
| `COLLECTION_SUBSCRIPTION_POSTS_PILOT_MIN_COMMUNITIES` | `50` | Минимальное наблюдение для разрешающего Gate B |

### Pilot и артефакты

| Переменная | По умолчанию | Описание |
|---|---|---|
| `COLLECTION_PILOT_SEED` | `20260728` | Seed детерминированной pilot-выборки |
| `COLLECTION_PILOT_GROUPS_PER_CATEGORY` | `10` | Групп каждой категории в pilot |
| `COLLECTION_EXPORT_DIR` | `/app/exports/stage2-pilot` | Pilot/capacity artifacts |

Изменение scopes или лимитов после pilot меняет collection-конфигурацию и аннулирует
соответствие capacity report. Пересборка image для изменения `.env` не нужна, но нужен
новый pilot.

## Модель данных

### Поиск и классификация

| Таблица | Назначение | Ключевая защита |
|---|---|---|
| `group_candidates` | Одна актуальная запись на группу VK | уникальный положительный `vk_id`, статус и confidence constraints |
| `search_keywords` | Ключевые слова и области | уникальная пара `subject + keyword` |
| `search_runs` | История запусков поиска | status, configuration, aggregate counters |
| `search_run_keywords` | Checkpoint каждого ключа и типа | уникальная тройка run/keyword/type, неотрицательный offset |
| `search_run_groups` | Дедуплицированные группы конкретного run | уникальная пара run/group, признак `was_new` |
| `group_keyword_matches` | Все совпадения группы | уникальная пара group/keyword |
| `classification_batches` | Заголовок immutable batch | уникальный публичный `batch_id` |
| `classification_batch_items` | Состав и порядок batch | уникальные batch/group и batch/position |
| `group_labels` | Multi-label результат | уникальная пара group/label и CHECK допустимых меток |
| `classification_reviews` | Аудит reclassification | уникальная пара operation/group |

### Сбор

| Таблица | Назначение | Ключевая защита |
|---|---|---|
| `collection_runs` | План, статус, конфигурация и счётчики run | status enum и `plan_key` в JSONB |
| `collection_jobs` | PostgreSQL-очередь, lease и checkpoint | уникальная задача run/type/entity |
| `collection_job_errors` | Очищенная диагностика ошибок | без токенов и сырых ответов |
| `group_collection_states` | Последний checkpoint и состояние группы | один row на группу |
| `group_posts` | Нормализованные посты | уникальная пара VK owner/post |
| `post_attachments` | Метаданные вложений | уникальная позиция в посте |
| `vk_users` | Минимальный публичный профиль | натуральный primary key `vk_id` |
| `group_memberships` | Связь группа–пользователь | уникальная пара group/user |
| `user_group_subscriptions` | Публичная подписка пользователя | уникальная пара user/VK group |

Все timestamp-колонки используют `timestamptz`. Сырые ответы VK не сохраняются.

### Статусы

Кандидат:

```text
pending → approved
pending → rejected
```

Поисковый run/keyword:

```text
running → paused → running
running → completed
running → failed
```

Collection job:

```text
pending → running → completed
                  ↘ retry_wait → pending
                  ↘ failed → pending (явный retry-failed)
pending/retry_wait → paused → pending
```

Collection run может иметь статусы `planned`, `running`, `paused`,
`paused_no_tokens`, `paused_capacity_limit`, `completed`, `completed_with_errors`,
`failed` и `cancelled`.

## Надёжность и идемпотентность

- PostgreSQL, а не Python-процесс, обеспечивает уникальность VK ID и связей.
- Сохранение страницы поиска и нового offset выполняется одной транзакцией.
- План поиска возобновляется только при совпадающем `plan_key`.
- Повторный поиск обновляет публичные поля и `last_seen_at`, не создавая дубли.
- Batch классификации фиксирует состав до ручной обработки.
- Импорт блокирует batch и группы, валидирует весь документ и коммитит один раз.
- Collection plan имеет `plan_key` из group IDs, scopes и лимитов.
- Claim задачи выполняется атомарно через `SKIP LOCKED`.
- Lease и heartbeat позволяют вернуть зависшее задание после падения worker.
- Checkpoint сохраняется после каждой страницы API.
- Upsert обновляет существующие сущности и сохраняет first/last seen.
- Foreground и autonomous worker используют одну и ту же очередь и модель прогресса.
- Тесты не требуют реальных VK-токенов и не ждут реальные минуты.

## Безопасность и приватность

### Секреты

- `.env`, `secrets/`, backups, exports и deploy-артефакты исключены из Git;
- token file монтируется в контейнер read-only;
- токены не записываются в БД;
- `TokenPool.__repr__` показывает только число токенов;
- HTTP/httpcore logs понижены до WARNING;
- Telegram token хранится только в runtime environment;
- production image не содержит `.env` и token file.

Проверка перед коммитом:

```bash
git status --short
git diff --cached --check
git diff --cached
```

### Минимизация данных

Для пользователя сохраняются только:

- VK ID;
- имя и фамилия;
- screen name;
- признаки закрытого, доступного или деактивированного профиля;
- технические UTC timestamps.

Не сохраняются телефон, email, адрес пользователя, родственники, взгляды, религия,
личные сообщения и бинарные фотографии. Для вложений хранятся только нормализованные
метаданные.

Безопасная инспекция:

```bash
docker compose run --rm collector privacy inspect-user VK_ID
docker compose run --rm collector privacy inspect-group VK_ID
```

Удаление пользователя:

```bash
make backup PURPOSE=before-user-delete
docker compose run --rm collector privacy delete-user VK_ID --confirm
```

Удаление выполняется одной транзакцией; зависимые memberships/subscriptions удаляются
по `ON DELETE CASCADE`.

### PostgreSQL

Не публикуйте порт без необходимости:

```dotenv
POSTGRES_BIND_ADDRESS=127.0.0.1
```

Безопасный доступ с рабочей станции:

```bash
ssh -L 15432:127.0.0.1:5432 USER@SERVER
psql 'postgresql://vk_reader:PASSWORD@127.0.0.1:15432/vk_research'
```

Значение `0.0.0.0` публикует PostgreSQL на всех интерфейсах. Если это временно
необходимо, ограничьте TCP/5432 firewall-правилами до доверенных IP.

Read-only роль создаётся init-скриптом только при первой инициализации PostgreSQL
volume. Изменение `POSTGRES_READER_PASSWORD` на существующем volume само по себе не
меняет пароль роли — выполните `ALTER ROLE` вручную.

## Разработка и тестирование

### Локальная Python-среда

Linux/macOS:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Для нативного CLI задайте доступный PostgreSQL через `DATABASE_URL`, путь к token file
через `VK_TOKENS_FILE` и запускайте:

```bash
collector --help
collector groups summary
```

### Обязательные проверки

Перед завершением изменения должны пройти:

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

Полный smoke test:

```bash
sh scripts/deploy-smoke.sh
```

Он проверяет Compose, строит image, запускает PostgreSQL, применяет Alembic, запускает
контейнерные тесты, проверяет CLI и `pg_isready`.

### Наборы тестов

- `tests/unit/` — VK client, token pool, поиск, очередь и worker на fake API;
- `tests/integration/test_postgres_workflow.py` — поиск и классификация с PostgreSQL;
- `tests/integration/test_stage2_workflow.py` — очередь, сбор и восстановление;
- `tests/test_config.py` — структура и уникальность keywords;
- `tests/test_classification_schemas.py` — JSON-контракты;
- `tests/test_cicd_contract.py` — инфраструктурные и CI/CD-инварианты.

Integration tests включаются в Compose через `RUN_INTEGRATION_TESTS=1`. Реальные VK
tokens не используются: ответы и время подменяются.

## Эксплуатация на Debian 12

Краткая последовательность для development-подобной установки:

```bash
sudo apt-get update
sudo apt-get install -y git curl
git clone <PRIVATE_REPOSITORY_URL> /opt/vk-research-collector
cd /opt/vk-research-collector
sudo sh scripts/create-swap.sh
cp .env.example .env
install -d -m 700 secrets exports backups
install -m 600 /dev/null secrets/vk_tokens.txt
chmod 600 .env
```

Установите Docker Engine и Compose plugin по официальной инструкции Docker. Затем
настройте `.env`, token file и выполните:

```bash
docker compose up -d postgres
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector --help
```

Compose настроен для слабого сервера:

- PostgreSQL: `shared_buffers=128MB`, `work_mem=4MB`,
  `maintenance_work_mem=64MB`, `max_connections=20`;
- лимит PostgreSQL: 512 MB и 0.75 CPU;
- лимит collector/worker: 384 MB и 0.75 CPU;
- Docker JSON logs: максимум 5 файлов по 10 MB;
- healthcheck PostgreSQL через `pg_isready`.

### Контроль диска

`scripts/disk-guard.sh`:

- при 85% удаляет только старые `/tmp`-файлы и старые ротируемые логи;
- при 95% создаёт `/var/lib/vk-research-collector/disk-stop` и возвращает ошибку;
- никогда не удаляет PostgreSQL data directory.

Пример cron каждые 10 минут:

```cron
*/10 * * * * set -a; . /opt/vk-research-collector/.env; set +a; /opt/vk-research-collector/scripts/disk-guard.sh
```

Полный production bootstrap рассчитан минимум на 20 GB и описан в
[`docs/SERVER_BOOTSTRAP.md`](docs/SERVER_BOOTSTRAP.md).

## Резервное копирование и восстановление

### Проверенный backup через Make

```bash
make backup PURPOSE=before-migration
```

Создаётся файл:

```text
backups/stage2-before-migration-YYYYMMDD-HHMMSSZ.dump
```

Команда использует `pg_dump -Fc`, проверяет ненулевой размер и запускает
`pg_restore --list` в отдельном PostgreSQL 16 container.

Ручной вариант:

```bash
docker compose exec -T postgres \
  pg_dump -U vk_collector -d vk_research -Fc > backups/manual.dump
docker run --rm -i postgres:16-alpine \
  pg_restore --list < backups/manual.dump > /dev/null
```

### Восстановление

Восстановление — деструктивная аварийная операция. Сначала остановите worker, создайте
ещё один backup текущего состояния и проверьте выбранный dump. Не удаляйте Docker
volume. Полная процедура находится в
[`docs/DEPLOYMENT_ROLLBACK.md`](docs/DEPLOYMENT_ROLLBACK.md).

### Перенос базы Windows → Debian

Одноразовый handoff автоматизирован скриптами:

```powershell
.\scripts\export-server-handoff.ps1 `
  -ServerUser deploy `
  -ServerHost 1.2.3.4 `
  -RemoteDirectory /opt/vk-research-collector/backups `
  -RunId 9be2813e-e1de-4ac9-bc07-7d92ac82438c
```

На сервере:

```bash
sudo -u deploy ./scripts/import-server-handoff.sh \
  backups/server-handoff-....dump \
  backups/server-handoff-....manifest.json \
  --confirm-replace-database
```

Никогда не запускайте локальный и серверный worker одновременно. Подробности — в
[`docs/DATABASE_HANDOFF.md`](docs/DATABASE_HANDOFF.md).

## CI/CD

Quality workflow выполняет:

- Ruff lint и format check;
- mypy strict;
- unit tests;
- secret scan;
- PostgreSQL integration tests;
- отдельный Compose smoke test.

Production workflow:

1. работает только для `main`;
2. собирает image и публикует его в GHCR;
3. разворачивает immutable tag `sha-<full commit SHA>`;
4. использует repository-scoped self-hosted runner;
5. не передаёт runtime secrets через GitHub;
6. выполняет preflight, backup, Alembic, healthcheck и проверку прогресса;
7. при проблеме возвращает предыдущий image;
8. не выполняет автоматический Alembic downgrade.

Runtime secrets остаются только на сервере:

```text
/opt/vk-research-collector/.env
/opt/vk-research-collector/secrets/vk_tokens.txt
```

Подробная настройка: [`docs/GITHUB_ACTIONS_DEPLOYMENT.md`](docs/GITHUB_ACTIONS_DEPLOYMENT.md).

## Диагностика

### Endpoint-aware подписки пользователей (выключены по умолчанию)

Новый контур не запускается автоматически. Каждый этап создаёт новый immutable run;
после создания run нельзя менять влияющие на него flags/лимиты. Перед Pilot A сделайте
backup, примените migration, включите только scope подписок и зафиксируйте лимит 50:

```bash
make backup PURPOSE=before-subscriptions-pilot
docker compose run --rm collector alembic upgrade head
docker compose run --rm collector collection subscriptions capacity-preview
export COLLECTION_SUBSCRIPTIONS_ENABLED=true
export COLLECTION_SUBSCRIPTIONS_MAX_PER_USER=50
export COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED=false
docker compose run --rm collector collection subscriptions pilot
```

`pilot` сам выполняет до 500 jobs и атомарно пишет
`subscription-gate-a.json`. Если report измеренный, свежий и разрешающий, создайте
отдельный production run, примените Gate A и выполните его:

```bash
docker compose run --rm collector collection subscriptions plan
docker compose run --rm collector collection capacity-apply \
  --run-id SUBSCRIPTIONS_RUN_ID \
  --source /app/exports/stage2-pilot/subscription-gate-a.json \
  --backup /app/backups/BEFORE_SUBSCRIPTIONS.dump
docker compose run --rm collector collection subscriptions run \
  --run-id SUBSCRIPTIONS_RUN_ID
```

Gate B — ещё один pilot и ещё один run. После включения posts нельзя продолжать
`SUBSCRIPTIONS_RUN_ID`, потому что его configuration неизменяема:

```bash
export COLLECTION_SUBSCRIPTION_GROUP_POSTS_ENABLED=true
docker compose run --rm collector collection subscriptions posts-pilot \
  --source-run-id PILOT_A_RUN_ID
docker compose run --rm collector collection subscriptions posts-plan \
  --source-run-id SUBSCRIPTIONS_RUN_ID
docker compose run --rm collector collection capacity-apply \
  --run-id POSTS_RUN_ID \
  --source /app/exports/stage2-pilot/subscription-gate-b.json \
  --backup /app/backups/BEFORE_SUBSCRIPTION_POSTS.dump
docker compose run --rm collector collection subscriptions run --run-id POSTS_RUN_ID
```

Перед production cohort обязательны backup, свободное место по реальному JSON report и
ручная фиксация gate. Теоретический preview никогда не выставляет
`production_allowed=true`. CLI сохраняет SHA-256, размер и mtime dump в production run и
повторно проверяет их перед первым API-вызовом. Сам dump предварительно проверьте через
`pg_restore --list`, как в разделе «Проверенный backup через Make». Ограничения endpoints
диагностируются без секретов:

```bash
docker compose run --rm collector collection method-limits
docker compose run --rm collector collection method-limits-reset --method groups.get --yes
```

### Контейнеры и логи

```bash
docker compose ps
docker compose logs --tail=200 postgres
docker compose logs --tail=200 collector-worker
docker compose logs -f collector-worker
```

### PostgreSQL не запускается

```bash
docker compose config
docker compose logs postgres
docker compose exec -T postgres pg_isready -U vk_collector -d vk_research
```

Проверьте `POSTGRES_PASSWORD`, занятость host-порта и права Docker volume. Не удаляйте
volume для «исправления» ошибки.

### Collector не видит token file

```bash
ls -l secrets/vk_tokens.txt
docker compose run --rm collector python -c \
  "from pathlib import Path; print(Path('/run/secrets/vk_tokens.txt').exists())"
```

Проверьте, что файл существует до `docker compose run`, непуст и содержит один токен на
строку. Значения токенов в диагностике не печатайте.

### Все токены отключены

Search run перейдёт в `paused`, collection run — в `paused_no_tokens`. Замените или
исправьте token file и выполните повторный `groups search` либо:

```bash
docker compose run --rm collector collection resume --run-id RUN_ID
```

### Run не запускается

```bash
docker compose run --rm collector collection status --run-id RUN_ID
docker compose run --rm collector collection verify --run-id RUN_ID
```

Проверьте:

- статус `paused_capacity_limit`;
- наличие успешного capacity report;
- совпадение runtime collection-конфигурации;
- заполнение диска;
- состояние токенов;
- не остались ли jobs в `retry_wait` или `failed`.

### Нет групп для экспорта

Это означает, что нет `pending`-групп, ещё не входивших в batch. Проверьте:

```bash
docker compose run --rm collector groups summary
docker compose run --rm collector classification summary
```

### Импорт отклонён

Типовые причины:

- неверный `batch_id`;
- пакет уже импортирован с другим результатом;
- дубли VK ID;
- отсутствуют решения полного формата;
- присутствуют ID, которых нет в batch;
- approved без labels или rejected с labels;
- confidence вне диапазона 0–1;
- файл находится на host, но не в смонтированном `exports`.

### Миграции расходятся со схемой

```bash
docker compose run --rm collector alembic current
docker compose run --rm collector alembic heads
docker compose run --rm collector alembic check
docker compose run --rm collector alembic upgrade head
```

Не выполняйте downgrade или пересоздание базы без проверенного backup и явного плана
восстановления.

## Структура репозитория

```text
.
├── alembic/                       # окружение и версии миграций
├── config/
│   └── keywords.yml              # области и поисковые ключи
├── docs/                          # архитектура, runbooks, отчёты и требования
├── scripts/                       # smoke, deploy, disk guard, backup handoff
├── src/vk_collector/
│   ├── classification/           # batch import/export, reclassification, audit
│   ├── cli/                      # Typer CLI
│   ├── collection/               # очередь, worker, отчёты, safety
│   ├── database/                 # SQLAlchemy models и sessions
│   ├── search/                   # orchestration и PostgreSQL persistence
│   ├── vk/                       # API client, token pool и модели ответов
│   ├── config.py                 # Pydantic settings и YAML loader
│   ├── privacy.py                # безопасная инспекция и удаление
│   └── subjects.py               # единый реестр областей
├── tests/                         # unit, integration и contract tests
├── .env.example                   # шаблон runtime-конфигурации
├── alembic.ini
├── compose.yaml                   # базовый Compose
├── compose.production.yaml        # production override с immutable image
├── Dockerfile
├── Dockerfile.postgres
├── Makefile
├── pyproject.toml
├── CLASSIFICATION_PROMPT.md
└── README.md
```

Runtime-каталоги `secrets/`, `exports/`, `backups/`, `logs/`, `.deploy/` и PostgreSQL
data не должны попадать в Git.

## Дополнительная документация

| Документ | Содержание |
|---|---|
| [`docs/PROJECT_CONTEXT.md`](docs/PROJECT_CONTEXT.md) | Контекст и текущая итерация |
| [`docs/ACCEPTANCE_CRITERIA.md`](docs/ACCEPTANCE_CRITERIA.md) | Критерии приёмки поиска и классификации |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Архитектура первого этапа |
| [`docs/STAGE2_REQUIREMENTS.md`](docs/STAGE2_REQUIREMENTS.md) | Требования контура approved-сбора |
| [`docs/STAGE2_ARCHITECTURE.md`](docs/STAGE2_ARCHITECTURE.md) | Queue, state machines и error policy |
| [`docs/STAGE2_DATA_MODEL.md`](docs/STAGE2_DATA_MODEL.md) | Подробная модель данных второго этапа |
| [`docs/STAGE2_CAPACITY_PLAN.md`](docs/STAGE2_CAPACITY_PLAN.md) | Методика pilot и оценки диска |
| [`docs/STAGE2_OPERATIONS.md`](docs/STAGE2_OPERATIONS.md) | Операционный порядок сбора |
| [`docs/STAGE2_PRIVACY.md`](docs/STAGE2_PRIVACY.md) | Минимизация персональных данных |
| [`docs/FOOD_SERVICE_IMPLEMENTATION_PLAN.md`](docs/FOOD_SERVICE_IMPLEMENTATION_PLAN.md) | План расширения «Общепит» |
| [`docs/FOOD_SERVICE_EXPANSION_REPORT.md`](docs/FOOD_SERVICE_EXPANSION_REPORT.md) | Состояние расширения |
| [`docs/OPERATIONS_DEBIAN12.md`](docs/OPERATIONS_DEBIAN12.md) | Базовая эксплуатация Debian 12 |
| [`docs/SERVER_BOOTSTRAP.md`](docs/SERVER_BOOTSTRAP.md) | Первичная production-настройка |
| [`docs/PRODUCTION_RUNBOOK.md`](docs/PRODUCTION_RUNBOOK.md) | Ежедневные production-операции |
| [`docs/GITHUB_ACTIONS_DEPLOYMENT.md`](docs/GITHUB_ACTIONS_DEPLOYMENT.md) | CI/CD и self-hosted runner |
| [`docs/TELEGRAM_MONITORING.md`](docs/TELEGRAM_MONITORING.md) | Alerts, daily report, setup и systemd timers |
| [`docs/DEPLOYMENT_ROLLBACK.md`](docs/DEPLOYMENT_ROLLBACK.md) | Image rollback и восстановление БД |
| [`docs/DATABASE_HANDOFF.md`](docs/DATABASE_HANDOFF.md) | Перенос рабочей PostgreSQL |

## Лицензия и доступ

Репозиторий приватный. Условия лицензирования и распространения отдельно не объявлены;
не считайте отсутствие файла `LICENSE` разрешением на публичное использование или
перераспространение.
