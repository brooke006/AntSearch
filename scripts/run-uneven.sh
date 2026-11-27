#!/usr/bin/env bash

source "$(dirname "$0")/experiment.sh"

export COMPOSE_PROFILES=many
run_each uneven
