# Common tasks. Run `make` or `make help` for the list.

PYTHON  ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
COMPOSE ?= docker compose
IMAGE   ?= antsearch:dev
PYTEST  := $(PYTHON) -m pytest

STARS   ?= 50

# Integration tests get their own compose project, port and a short heartbeat timeout,
# so they never touch a dev stack started with `make up`. Their workers run simulated tasks
# instead of downloading from MAST.
IT_PORT    ?= 8001
IT_COMPOSE := ANTSEARCH_PORT=$(IT_PORT) HEARTBEAT_TIMEOUT=3 WORKER_TASK=simulated SIMULATED_TASK_SECONDS=0.2 \
	POLL_INTERVAL=0.5 $(COMPOSE) -p antsearch-it

.DEFAULT_GOAL := help
.PHONY: help install build up down logs ps submit test test-unit test-science test-integration test-all coverage clean

help: ## Show this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-18s %s\n", $$1, $$2}'

install: ## Install the package with dev and science extras
	$(PYTHON) -m pip install -e ".[dev,science]"

build: ## Build the Docker image (IMAGE=antsearch:dev)
	docker build -t $(IMAGE) .

up: ## Start redis, the coordinator and two workers in the background on http://localhost:8000
	$(COMPOSE) up --build -d --wait

down: ## Stop the stack started by `make up`
	$(COMPOSE) down

logs: ## Follow the stack's logs
	$(COMPOSE) logs -f

ps: ## Show the stack's containers
	$(COMPOSE) ps

submit: ## Queue the first STARS stars (default 50) on the stack and wait for the results
	ANTSEARCH_URL=http://localhost:$${ANTSEARCH_PORT:-8000} $(PYTHON) -m antsearch.submit --limit $(STARS)

test: ## Unit and science tests: everything that doesn't need a running stack
	$(PYTEST) -m "not integration"

test-unit: ## Fast tests only: coordinator, worker, package
	$(PYTEST) -m "not science and not integration"

test-science: ## Light-curve pipeline tests (synthetic data, no network)
	$(PYTEST) -m science

test-integration: ## Start a throwaway stack on port 8001, test it over HTTP, then tear it down
	$(IT_COMPOSE) up --build -d --wait
	ANTSEARCH_URL=http://localhost:$(IT_PORT) $(PYTEST) -m integration; \
		status=$$?; $(IT_COMPOSE) down -v; exit $$status

test-all: test test-integration ## Every test, including integration

coverage: ## Unit and science tests with coverage: terminal summary plus htmlcov/index.html
	$(PYTEST) -m "not integration" --cov=antsearch --cov-report=term-missing --cov-report=html

clean: ## Remove test caches and coverage output
	rm -rf .pytest_cache htmlcov .coverage .coverage.* coverage.xml
	find . -name __pycache__ -type d -not -path './.venv/*' -prune -exec rm -rf {} +
