# Прогресс

- [x] Инвентаризация исходного репозитория и checkpoint-коммит.
- [x] Создана ветка `feat/group-discovery-classification`.
- [x] Созданы критерии приёмки, план и журнал решений.
- [x] Схема БД и миграции.
- [x] VK API и возобновляемый поиск.
- [x] Классификация, CLI и статистика.
- [x] Docker, CI/CD и документация Debian 12.
- [x] Финальный аудит и полный локальный/Docker-набор проверок.

Последний подтверждённый Docker-прогон: миграция `20260728_0001` применена,
включая PostgreSQL integration workflow; все 14 контейнерных тестов прошли.

## Второй этап

- [x] Начальный аудит, сверка классификации и идентификация 12 test fixtures.
- [x] Архитектура, требования, модель, план ёмкости и критерии приёмки.
- [x] Миграции `0002`/`0003`/`0004` и PostgreSQL queue с lease/SKIP LOCKED.
- [x] VK scopes, worker, CLI, privacy и наблюдаемость.
- [x] Fake/integration/smoke tests, Docker и CI.
- [x] Первый pilot выявил опасный прогноз 13,54 GiB; старый run оставлен в
  `paused_capacity_limit`.
- [x] Изолированный repilot 100 posts / 200 members: прогноз 3,89 GiB, gate passed.
- [x] Full run `9be2813e-e1de-4ac9-bc07-7d92ac82438c` запущен автономным
  `collector-worker`; реальный stop/start продолжил счётчики без дублей.
- [x] Миграция на новый Cloud.ru production сервер (2 vCPU, 4 GB RAM, 100 GB SSD, 9 GB Swap).
- [x] Настройка ротации логов Docker (`max-size: 20m`, `max-file: 3`), системного пользователя `deploy`, таймеров Telegram-мониторинга и `disk-guard.sh`.
- [x] Установка и регистрация нового GitHub Actions self-hosted runner `vk-collector-production-01`.
- [x] Модификация ML-пайплайна (`media_resolver.py`, `video_processor.py`): JIT URL refresh, safe photo sizes (`y`/`x` до 768px), fail-safe video fallback to preview, gentle downloads (`Semaphore(3)`, HTTP/2, jitter, backoff), ограничение CPU потоков до 1 ядра (`cv2.setNumThreads(1)`, `ffmpeg -threads 1`), потоковое удаление сырого видео.
- [x] Оптимизация `compose.production.yaml` под 4 GB RAM (`shared_buffers=768MB`, memory limits).
- [x] Деплой и перевод сервера в режим ожидания (Standby, фоновый сбор отключен до импорта дампа БД).
