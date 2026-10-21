.PHONY: up bi down run test psql

up:            ## Start Postgres + Airflow (UI on http://localhost:8080, admin/admin)
	docker compose up -d

bi:            ## Also start Metabase on http://localhost:3000
	docker compose --profile bi up -d

down:          ## Stop everything (add -v to wipe data: docker compose down -v)
	docker compose down

run:           ## Run one week through the pipeline without Airflow
	python -m pipeline.run --start 2026-10-01 --end 2026-10-07

test:
	pytest -q

psql:          ## Open a SQL prompt on the warehouse
	docker compose exec postgres psql -U warehouse -d warehouse
