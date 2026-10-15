#!/usr/bin/env bash
# End-to-end pipeline runner for the SME onboarding & servicing project.
#
# Stages:  [download] -> ingest/filter -> narratives -> hive tables
#          -> validate (GATE: pipeline stops on critical failure) -> train classifier
#
# Usage (from anywhere):
#   bash scripts/run_pipeline.sh                   # full run
#   bash scripts/run_pipeline.sh --skip-train      # data layers + validation only
#   bash scripts/run_pipeline.sh --with-download   # re-download narrative archive first
#
# Logs:
#   logs/pipeline.log              key=value event lines (Splunk-friendly)
#   logs/<run_id>_<step>.log       full stdout/stderr of each step
set -euo pipefail

cd "$(dirname "$0")/.."            # always run from the repo root

RUN_ID="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="logs"
EVENT_LOG="$LOG_DIR/pipeline.log"
mkdir -p "$LOG_DIR"

SKIP_TRAIN=0
WITH_DOWNLOAD=0
for arg in "$@"; do
  case "$arg" in
    --skip-train)    SKIP_TRAIN=1 ;;
    --with-download) WITH_DOWNLOAD=1 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

# Prevent overlapping runs (e.g. cron firing while a manual run is active)
exec 9>"$LOG_DIR/pipeline.lock"
if ! flock -n 9; then
  echo "Another pipeline run is already in progress. Exiting." >&2
  exit 1
fi

# shellcheck disable=SC1091
source venv/bin/activate

log_event() {
  echo "ts=$(date -Iseconds) run_id=$RUN_ID $*" | tee -a "$EVENT_LOG"
}

on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    log_event "pipeline status=FAILED exit_code=$rc"
  fi
}
trap on_exit EXIT

run_step() {
  local name="$1"; shift
  local step_log="$LOG_DIR/${RUN_ID}_${name}.log"
  local start
  start="$(date +%s)"
  log_event "step=$name status=START"
  if "$@" > "$step_log" 2>&1; then
    log_event "step=$name status=SUCCESS duration_s=$(( $(date +%s) - start ))"
  else
    local rc=$?
    log_event "step=$name status=FAILED exit_code=$rc duration_s=$(( $(date +%s) - start )) log=$step_log"
    exit "$rc"
  fi
}

log_event "pipeline status=START skip_train=$SKIP_TRAIN with_download=$WITH_DOWNLOAD"

if [ "$WITH_DOWNLOAD" -eq 1 ]; then
  run_step download bash scripts/download_narratives.sh
fi

run_step ingest_filter     python etl/profile_cfpb.py
run_step build_narratives  python etl/build_narratives_layer.py
run_step hive_tables       python etl/create_hive_tables.py
run_step validate          python etl/validate_data.py     # exits 1 on a critical failure -> stops here

if [ "$SKIP_TRAIN" -eq 0 ]; then
  run_step train_classifier python etl/train_ticket_classifier.py
fi

log_event "pipeline status=SUCCESS"
