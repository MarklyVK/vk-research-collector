.PHONY: search-groups groups-summary export-classification import-classification classification-summary start-collection collection-plan collection-pilot collection-run collection-status collection-pause collection-resume collection-retry-failed collection-verify collection-summary backup migrate test lint logs up down smoke

RUN = docker compose run --rm collector

search-groups:
	$(RUN) groups search
groups-summary:
	$(RUN) groups summary
export-classification:
	$(RUN) classification export
import-classification:
	@test -n "$(FILE)" || (echo "Укажите FILE=results.json" && exit 2)
	$(RUN) classification import $(FILE)
classification-summary:
	$(RUN) classification summary
start-collection:
	$(RUN) collection start
collection-plan:
	$(RUN) collection plan $(if $(APPLY),--apply,)
collection-pilot:
	$(RUN) collection pilot
collection-run:
	$(RUN) collection run $(if $(RUN_ID),--run-id $(RUN_ID),) --until-idle
collection-status:
	$(RUN) collection status $(if $(RUN_ID),--run-id $(RUN_ID),)
collection-pause:
	@test -n "$(RUN_ID)" || (echo "Укажите RUN_ID" && exit 2)
	$(RUN) collection pause --run-id $(RUN_ID)
collection-resume:
	@test -n "$(RUN_ID)" || (echo "Укажите RUN_ID" && exit 2)
	$(RUN) collection resume --run-id $(RUN_ID)
collection-retry-failed:
	@test -n "$(RUN_ID)" || (echo "Укажите RUN_ID" && exit 2)
	$(RUN) collection retry-failed --run-id $(RUN_ID)
collection-verify:
	@test -n "$(RUN_ID)" || (echo "Укажите RUN_ID" && exit 2)
	$(RUN) collection verify --run-id $(RUN_ID)
collection-summary:
	$(RUN) collection summary
backup:
	@test -n "$(PURPOSE)" || (echo "Укажите PURPOSE" && exit 2)
	@mkdir -p backups
	@name="stage2-$(PURPOSE)-$$(date -u +%Y%m%d-%H%M%SZ).dump"; \
	  docker compose exec -T postgres pg_dump -U "$${POSTGRES_USER:-vk_collector}" -d "$${POSTGRES_DB:-vk_research}" -Fc > "backups/$$name"; \
	  test -s "backups/$$name"; \
	  docker run --rm -v "$$PWD/backups:/backups:ro" postgres:16-alpine pg_restore --list "/backups/$$name" >/dev/null; \
	  echo "Backup создан и проверен: backups/$$name"
migrate:
	$(RUN) alembic upgrade head
test:
	$(RUN) pytest -q
lint:
	$(RUN) ruff check .
	$(RUN) ruff format --check .
	$(RUN) mypy src
logs:
	docker compose logs --tail=200 -f
up:
	docker compose up -d postgres
down:
	docker compose down
smoke:
	sh scripts/deploy-smoke.sh
