
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=$([ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
STARS=${STARS:-50}
SCHEDULERS=${SCHEDULERS:-round-robin random greedy ant}
export COMPOSE_PROJECT_NAME=antsearch-experiment
export ANTSEARCH_PORT=8002
# The workers read this machine's light-curve cache (compose.experiment.yaml), so transit runs compute without downloading.
export COMPOSE_FILE=compose.yaml:compose.experiment.yaml
export WORKER_TASK=${WORKER_TASK:-transit}
export SIMULATED_TASK_SECONDS=${SIMULATED_TASK_SECONDS:-2}

stop() { docker compose --profile many down -v "$@"; }
trap 'stop > /dev/null 2>&1' EXIT

wait_for_workers() {
	local expected
	expected=$(docker compose ps --services | grep -cv -e '^coordinator$' -e '^redis$')
	until [ "$(curl -s "localhost:$ANTSEARCH_PORT/workers" | grep -o '"status":"online"' | wc -l)" -ge "$expected" ]; do
		sleep 1
	done
	echo "$expected workers registered"
}

# run_each <condition> [command to run while the stars are being worked on]
run_each() {
	local condition=$1
	shift
	export RUN_LOG_DIR=./output/experiments/$condition
	mkdir -p "$RUN_LOG_DIR"
	local logs=()
	for scheduler in $SCHEDULERS; do
		echo "=== $condition: $scheduler"
		stop > /dev/null 2>&1
		local before
		before=$(ls "$RUN_LOG_DIR")
		SCHEDULER=$scheduler make up
		wait_for_workers
		[ $# -eq 0 ] || "$@" &
		make submit STARS="$STARS" | tail -n 3
		wait
		stop
		logs+=("$RUN_LOG_DIR/$(comm -13 <(echo "$before") <(ls "$RUN_LOG_DIR") | head -n 1)")
	done
	for log in "${logs[@]}"; do
		make --no-print-directory metrics RUN="$log"
		echo
	done
}
