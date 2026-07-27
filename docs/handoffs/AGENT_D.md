# Handoff: Docker, CI/CD и эксплуатация

Созданы Dockerfile, Compose для `collector`/PostgreSQL, Makefile, CI и deploy workflows, read-only init, swap, disk guard, deploy smoke и инструкция Debian 12.

Интегратору нужно сверить имя console script `vk-collector` и extra `.[dev]` с итоговым `pyproject.toml`. Контракт остановки тяжёлых заданий — наличие `/var/lib/vk-research-collector/disk-stop`; application-код роли D не менялся.
