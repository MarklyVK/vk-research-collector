.PHONY: search-groups groups-summary export-classification import-classification classification-summary start-collection migrate test lint logs up down smoke

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
