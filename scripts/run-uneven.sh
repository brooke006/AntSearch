#!/usr/bin/env bash

source "$(dirname "$0")/experiment.sh"

export WORKER_2_SLOWDOWN=2 WORKER_3_SLOWDOWN=4
run_each uneven
