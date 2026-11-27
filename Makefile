PYTHON  ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
COMPOSE ?= docker compose
IMAGE   ?= antsearch:dev
PYTEST  := $(PYTHON) -m pytest

STARS   ?= 50
COPIES  ?= 1
PREFETCH ?=
PREFETCH_COUNTS ?= 0 15 30 50 100
# make machines WORKERS=N instead starts N plain workers, SLOW of them SLOW_FACTOR times slower
WORKERS ?=
SLOW    ?= 0
SLOW_FACTOR ?= 8
SCENARIO ?=
RUN     ?=

# Local IP for multi-machine workers
HOST_IP  = $(shell ipconfig getifaddr en0 2>/dev/null || hostname -I 2>/dev/null | cut -d' ' -f1)
MACHINES := $(COMPOSE) -f compose.machines.yaml

# Isolated compose stack for integration tests
IT_PORT    ?= 8001
IT_RUNS    := ./output/runs-it
IT_COMPOSE := ANTSEARCH_PORT=$(IT_PORT) RUN_LOG_DIR=$(IT_RUNS) HEARTBEAT_TIMEOUT=3 WORKER_TASK=simulated SIMULATED_TASK_SECONDS=0.2 \
	POLL_INTERVAL=0.5 $(COMPOSE) -p antsearch-it

.DEFAULT_GOAL := help
.PHONY: help install build up coordinator worker machines down logs ps submit scenario metrics recall save-run sweep test test-unit test-science test-integration test-all coverage clean

help: ## Show this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-18s %s\n", $$1, $$2}'

install: ## Install the package with dev and science extras
	$(PYTHON) -m pip install -e ".[dev,science]"

build: ## Build the Docker image (IMAGE=antsearch:dev)
	docker build -t $(IMAGE) .

up: ## Start redis, the coordinator and three workers in the background on http://localhost:8000
	mkdir -p $${RUN_LOG_DIR:-output/runs}
	$(COMPOSE) up --build -d --wait

coordinator: ## Start only redis and the coordinator, for workers on other machines to join
	mkdir -p $${RUN_LOG_DIR:-output/runs}
	$(COMPOSE) up --build -d --wait redis coordinator
	@echo "Workers on other machines join with:"
	@echo "  make worker COORDINATOR_URL=http://$(HOST_IP):$${ANTSEARCH_PORT:-8000} WORKER_ID=<name>"
	@ip=$$(tailscale ip -4 2>/dev/null | head -1); test -z "$$ip" || \
		echo "or, from machines on your Tailscale network, COORDINATOR_URL=http://$$ip:$${ANTSEARCH_PORT:-8000}"

worker: ## Run a worker here: make worker COORDINATOR_URL=http://<address>:8000 WORKER_ID=<name>
	@test -n "$(COORDINATOR_URL)" && test -n "$(WORKER_ID)" || \
		{ echo "usage: make worker COORDINATOR_URL=http://<address>:8000 WORKER_ID=<a name no other machine uses>"; exit 2; }
	COORDINATOR_URL=$(COORDINATOR_URL) WORKER_ID=$(WORKER_ID) $(PYTHON) -m antsearch.worker

machines: ## Start worker containers with different CPU/memory limits (COPIES=N, PREFETCH=1), or WORKERS=N SLOW=M plain ones

ifdef WORKERS
# Named fast and slow worker containers sharing this machine's light-curve cache, e.g. to join
# someone else's experiment: make machines WORKERS=20 SLOW=8 COORDINATOR_URL=http://<their address>:8002
machines:
	@test "$(SLOW)" -le "$(WORKERS)" || { echo "SLOW ($(SLOW)) can't be more than WORKERS ($(WORKERS))"; exit 2; }
	docker build -q -t $(IMAGE) . > /dev/null
	@mkdir -p $$HOME/.cache/lightkurve
	@prefix=$${MACHINE_PREFIX:-$${USER:-local}}; \
	url=$${COORDINATOR_URL:-http://$(HOST_IP):$${ANTSEARCH_PORT:-8000}}; \
	echo "joining $$url"; \
	start() { \
		docker rm -f $$1 > /dev/null 2>&1; \
		docker run -d --name $$1 --label antsearch.machines \
			-e COORDINATOR_URL=$$url -e WORKER_ID=$$1 -e SLOWDOWN=$$2 \
			-e WORKER_TASK=$${WORKER_TASK:-transit} -e POLL_INTERVAL=$${POLL_INTERVAL:-0.5} \
			-v $$HOME/.cache/lightkurve:/home/antsearch/.cache/lightkurve \
			$(IMAGE) > /dev/null && echo "  started $$1"; \
	}; \
	for i in $$(seq 1 $$(($(WORKERS) - $(SLOW)))); do start $$prefix-fast-$$i 1 || exit 1; done; \
	for i in $$(seq 1 $(SLOW)); do start $$prefix-slow-$$i $(SLOW_FACTOR) || exit 1; done
else
machines:
	@for n in $$(seq 1 $(COPIES)); do \
		if [ $$n -eq 1 ]; then \
			project=antsearch-machines; \
			prefix=$${MACHINE_PREFIX:-$${USER:-local}}; \
			build="--build"; \
		else \
			project=antsearch-machines-$$n; \
			prefix=$${MACHINE_PREFIX:-$${USER:-local}}$$n; \
			build=""; \
		fi; \
		echo "starting $$project"; \
		counts=$$([ -z "$(PREFETCH)" ] || $(PYTHON) -c 'import random, sys; c = sys.argv[1:]; random.shuffle(c); print(*c)' $(PREFETCH_COUNTS)); \
		set -- $$counts; \
		[ -z "$$counts" ] || echo "  cached stars: desktop $$1, laptop $$2, old-laptop $$3, netbook $$4, potato $$5"; \
		DESKTOP_PREFETCH=$$1 LAPTOP_PREFETCH=$$2 OLD_LAPTOP_PREFETCH=$$3 NETBOOK_PREFETCH=$$4 POTATO_PREFETCH=$$5 \
		MACHINE_PREFIX=$$prefix \
		COORDINATOR_URL=$${COORDINATOR_URL:-http://$(HOST_IP):$${ANTSEARCH_PORT:-8000}} \
			$(MACHINES) -p $$project up -d $$build || exit 1; \
	done
endif

down: ## Stop the compose stack, all worker machines and the VS Code debug Redis
	@for project in $$($(COMPOSE) ls --all --quiet --filter name=antsearch-machines); do \
		echo "stopping $$project"; $(MACHINES) -p $$project down; \
	done
	@ids=$$(docker ps -aq --filter label=antsearch.machines); \
	[ -z "$$ids" ] || { docker rm -f $$ids > /dev/null && echo "removed $$(echo $$ids | wc -w | tr -d ' ') WORKERS= containers"; }
	$(COMPOSE) down
	@# Started by the coordinator debug configs (.vscode/tasks.json); the next debug launch makes a fresh one.
	@! docker inspect antsearch-debug-redis > /dev/null 2>&1 || { docker rm -f antsearch-debug-redis > /dev/null && echo "removed antsearch-debug-redis"; }

logs: ## Follow the stack's logs
	$(COMPOSE) logs -f

ps: ## Show the stack's containers
	$(COMPOSE) ps

submit: ## Queue the first STARS stars (default 50) on the stack and wait for the results
	ANTSEARCH_URL=http://localhost:$${ANTSEARCH_PORT:-8000} $(PYTHON) -m antsearch.submit --limit $(STARS)

scenario: ## Run a failure scenario on the stack, e.g. SCENARIO=scenarios/slow-worker.yaml
	@test -n "$(SCENARIO)" || { echo "usage: make scenario SCENARIO=scenarios/<name>.yaml"; exit 2; }
	$(PYTHON) -m antsearch.scenario $(SCENARIO) --log output/scenarios/$(basename $(notdir $(SCENARIO))).jsonl

metrics: ## Completion time, throughput and wasted work of the newest run, or of RUN=output/runs/<run>.jsonl
	$(PYTHON) -m antsearch.metrics $(RUN)

recall: ## Planet recall of the newest run (or RUN=...)
	$(PYTHON) -m antsearch.recall $(RUN)

save-run: ## Copy the newest run log (or RUN=...) into docs/runs/, with its metrics and recall
	$(PYTHON) -m antsearch.metrics --save docs/runs $(RUN)
	$(PYTHON) -m antsearch.recall --save docs/runs $(RUN)

sweep: ## Simulate the ant scheduler over a grid of alpha, beta and evaporation; writes docs/parameter-sweep-*
	$(PYTHON) -m antsearch.sweep

test: ## Unit and science tests: everything that doesn't need a running stack
	$(PYTEST) -m "not integration"

test-unit: ## Fast tests only: coordinator, replication, worker, submit, scenarios, run logs, pheromone table, package
	$(PYTEST) -m "not science and not integration"

test-science: ## Light-curve pipeline tests (synthetic data, no network)
	$(PYTEST) -m science

test-integration: ## Start a throwaway stack on port 8001, test it over HTTP, then tear it down
	mkdir -p $(IT_RUNS)
	rm -f $(IT_RUNS)/*.jsonl
	$(IT_COMPOSE) up --build -d --wait
	ANTSEARCH_URL=http://localhost:$(IT_PORT) ANTSEARCH_COMPOSE_PROJECT=antsearch-it ANTSEARCH_RUN_LOG_DIR=$(IT_RUNS) $(PYTEST) -m integration; \
		status=$$?; $(IT_COMPOSE) down -v; exit $$status

test-all: test test-integration ## Every test, including integration

coverage: ## Unit and science tests with coverage: terminal summary plus htmlcov/index.html
	$(PYTEST) -m "not integration" --cov=antsearch --cov-report=term-missing --cov-report=html

clean: ## Remove test caches and coverage output
	rm -rf .pytest_cache htmlcov .coverage .coverage.* coverage.xml
	find . -name __pycache__ -type d -not -path './.venv/*' -prune -exec rm -rf {} +
