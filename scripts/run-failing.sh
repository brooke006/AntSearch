#!/usr/bin/env bash

source "$(dirname "$0")/experiment.sh"

run_each failing "$PYTHON" -m antsearch.scenario scenarios/crash-and-recover.yaml --project "$COMPOSE_PROJECT_NAME"
