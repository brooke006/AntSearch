
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=$([ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
STARS=${STARS:-50}
SCHEDULERS=${SCHEDULERS:-round-robin random greedy ant}
export COMPOSE_PROJECT_NAME=antsearch-experiment
export ANTSEARCH_PORT=8002
export WORKER_TASK=${WORKER_TASK:-simulated}
export SIMULATED_TASK_SECONDS=${SIMULATED_TASK_SECONDS:-2}

trap 'docker compose down -v > /dev/null 2>&1' EXIT

# run_each <condition> [command to run while the stars are being worked on]
run_each() {
	local condition=$1
	shift
	export RUN_LOG_DIR=./output/experiments/$condition
	mkdir -p "$RUN_LOG_DIR"
	local logs=()
	for scheduler in $SCHEDULERS; do
		echo "=== $condition: $scheduler"
		docker compose down -v > /dev/null 2>&1
		local before
		before=$(ls "$RUN_LOG_DIR")
		SCHEDULER=$scheduler make up
		[ $# -eq 0 ] || "$@" &
		make submit STARS="$STARS" | tail -n 3
		wait
		docker compose down -v
		logs+=("$RUN_LOG_DIR/$(comm -13 <(echo "$before") <(ls "$RUN_LOG_DIR") | head -n 1)")
	done
	for log in "${logs[@]}"; do
		make --no-print-directory metrics RUN="$log"
		echo
	done
}
