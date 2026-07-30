# Первоначальная настройка production-сервера

Все шаги этого документа выполняются один раз. Debian 12 сервер должен иметь минимум
20 GB диска. До handoff локальный worker остаётся единственным активным worker.

## 1. Docker и системный пользователь

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git python3
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo useradd --create-home --shell /bin/bash deploy
sudo groupadd --system vkcollector
sudo usermod -aG docker,vkcollector deploy
sudo install -d -o deploy -g vkcollector -m 2770 /opt/vk-research-collector
sudo install -d -o deploy -g deploy -m 0700 \
  /opt/vk-research-collector/{backups,.deploy,runner}
sudo install -d -o 10001 -g vkcollector -m 2710 \
  /opt/vk-research-collector/secrets
sudo install -d -o 10001 -g vkcollector -m 2770 \
  /opt/vk-research-collector/exports
```

Перезайдите в shell пользователя `deploy`, чтобы применилось членство в `docker`.

## 2. Bootstrap-файлы и runtime-секреты

Из доверенного checkout exact commit скопируйте только начальные runtime-файлы:

```bash
sudo install -o deploy -g deploy -m 0644 compose.yaml compose.production.yaml \
  /opt/vk-research-collector/
sudo install -d -o deploy -g vkcollector -m 2755 \
  /opt/vk-research-collector/{config,scripts}
sudo install -o deploy -g deploy -m 0644 config/keywords.yml \
  /opt/vk-research-collector/config/keywords.yml
sudo install -o deploy -g deploy -m 0755 scripts/postgres-init-readonly.sh \
  scripts/deploy-production.sh scripts/import-server-handoff.sh \
  /opt/vk-research-collector/scripts/
sudo install -o 10001 -g 10001 -m 0600 /dev/null \
  /opt/vk-research-collector/secrets/vk_tokens.txt
sudo install -o deploy -g deploy -m 0600 .env.example \
  /opt/vk-research-collector/.env
```

Отредактируйте `/opt/vk-research-collector/.env`: `APP_ENV=production`, стойкие
уникальные PostgreSQL passwords, `POSTGRES_BIND_ADDRESS=127.0.0.1`,
`POSTGRES_VOLUME_NAME=vk_research_postgres_data` и основной `COLLECTION_RUN_ID`.
Заполните `secrets/vk_tokens.txt`, по одному VK token на строку, затем проверьте:

```bash
sudo chmod 600 /opt/vk-research-collector/.env \
  /opt/vk-research-collector/secrets/vk_tokens.txt
sudo chown deploy:deploy /opt/vk-research-collector/.env
sudo chown 10001:10001 /opt/vk-research-collector/secrets/vk_tokens.txt
sudo chown 10001:vkcollector /opt/vk-research-collector/secrets \
  /opt/vk-research-collector/exports
sudo chmod 2710 /opt/vk-research-collector/secrets
sudo chmod 2770 /opt/vk-research-collector/exports
sudo -u deploy test -f /opt/vk-research-collector/secrets/vk_tokens.txt
sudo -u deploy test ! -r /opt/vk-research-collector/secrets/vk_tokens.txt
sudo -u deploy test -w /opt/vk-research-collector/exports
```

## 3. Первый production image и PostgreSQL volume

После первого успешного `build-image` узнайте полный SHA-tag в Actions. Для
одноразового ручного pull private GHCR package используйте token только с
`read:packages`; не сохраняйте его в shell history:

```bash
read -r -s GHCR_TOKEN
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_LOGIN --password-stdin
unset GHCR_TOKEN
export COLLECTOR_IMAGE='ghcr.io/marklyvk/vk-research-collector/collector:sha-<FULL_SHA>'
docker pull "$COLLECTOR_IMAGE"
printf '%s\n' "$COLLECTOR_IMAGE" > /opt/vk-research-collector/.deploy/current-image
cd /opt/vk-research-collector
docker compose -f compose.yaml -f compose.production.yaml up -d postgres
docker volume inspect vk_research_postgres_data
```

Если сервер уже использует старый `vk-research-collector_postgres_data`, не создавайте
новый volume: задайте именно это имя в `POSTGRES_VOLUME_NAME`.

## 4. Runner, environment и handoff

Зарегистрируйте runner точной командой из
`docs/GITHUB_ACTIONS_DEPLOYMENT.md`, создайте GitHub Environment `production` только
для `main`, затем выполните database handoff по `docs/DATABASE_HANDOFF.md`.

После успешного импорта server worker уже запущен, а локальный остаётся остановлен.
Перезапустите failed production workflow или сделайте следующий push в `main`.
Дальнейшие deploy полностью автоматические.
