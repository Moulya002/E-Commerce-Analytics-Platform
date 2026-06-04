.PHONY: setup up down health seed demo logs spark-ui dashboard batch cdc airflow

setup:
	./scripts/setup.sh

up:
	docker compose up -d

down:
	./scripts/teardown.sh

health:
	./scripts/healthcheck.sh

seed:
	python3 scripts/seed_metrics.py

demo: setup
	@echo "Waiting 90s for pipeline..."
	sleep 90
	$(MAKE) health

logs:
	docker compose logs -f producer spark-streaming dashboard

spark-ui:
	@echo "http://localhost:8080"

dashboard:
	@echo "http://localhost:8501"

batch:
	./scripts/run_batch_etl.sh

cdc:
	docker compose --profile cdc up -d

airflow:
	docker compose --profile airflow up -d
