.PHONY: install lint test test-integration up down e2e loadgen kind-up kind-e2e kind-down

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy
	uv run mypy scripts tests

test:
	uv run pytest

test-integration:
	uv run pytest -m integration

up:
	docker compose up -d --build --wait

down:
	docker compose down -v

e2e:
	uv run pytest -m e2e

loadgen:
	@KEY=$$(curl -sf -XPOST localhost:8003/v1/projects -H 'Authorization: Bearer dev-admin-token' \
		-H 'Content-Type: application/json' -d '{"name":"loadgen"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["key"]["api_key"])'); \
	sleep 2; uv run python scripts/loadgen.py --api-key $$KEY --rps 5000 --duration 30

kind-up:
	./scripts/kind-up.sh

kind-e2e:
	./scripts/kind-e2e.sh

kind-down:
	kind delete cluster --name pulse
