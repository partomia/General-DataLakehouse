#!/usr/bin/env bash
# Create/sync the CDE Repository for this GitHub repo and (re)create one Spark job per
# pipeline stage, each reading its application file from the repo (mounted at /app/mount,
# so the jobs find config/, contracts/ and model/ next to cde/jobs/). The jobs need only
# PySpark and the standard library: no python-env resource.
#
# After a code change: git push, then re-run this script or just
#   cde repository sync --name rsingh-gdl-pipeline
#
# Resources are small (the data is a demo: about 1,000 customers): a 1-core / 2 GB driver
# and 1 to 2 executors of 2 cores / 4 GB, well inside the shared federal YuniKorn queue.
#
# This script does not delete jobs it no longer defines; remove orphans by hand.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/General-DataLakehouse}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-gdl-pipeline}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-gdl}"
DB_PREFIX="${DB_PREFIX:-rsingh_gdl}"
LANDING="${LANDING:-s3a://federal-buk-574bcea0/data/IB/rsingh_gdl/landing}"
RESOURCES=(--driver-cores "${DRIVER_CORES:-1}" --driver-memory "${DRIVER_MEMORY:-2g}"
           --executor-cores "${EXECUTOR_CORES:-2}" --executor-memory "${EXECUTOR_MEMORY:-4g}"
           --min-executors 1 --initial-executors 1 --max-executors "${MAX_EXECUTORS:-2}"
           --conf spark.sql.shuffle.partitions=8
           --conf spark.sql.adaptive.enabled=true)

echo "==> Repository: ${REPO_NAME}"
if cde repository describe --name "${REPO_NAME}" &>/dev/null; then
  echo "    exists, syncing ${REPO_BRANCH}"
else
  cde repository create --name "${REPO_NAME}" --url "${REPO_URL}" --branch "${REPO_BRANCH}"
fi
cde repository sync --name "${REPO_NAME}"

create_job() {
  local name=$1 file=$2
  if cde job describe --name "${name}" &>/dev/null; then
    cde job delete --name "${name}"
  fi
  echo "==> Creating job ${name} (${file})"
  cde job create --name "${name}" --type spark \
    --mount-1-resource "${REPO_NAME}" \
    --application-file "${file}" \
    "${RESOURCES[@]}" \
    --arg=--db-prefix --arg="${DB_PREFIX}" --arg=--landing --arg="${LANDING}"
}

create_job "${JOB_PREFIX}-land"   "cde/jobs/land_sources.py"
create_job "${JOB_PREFIX}-bronze" "cde/jobs/ingest_bronze.py"
create_job "${JOB_PREFIX}-silver" "cde/jobs/build_silver.py"
create_job "${JOB_PREFIX}-mdm"    "cde/jobs/build_mdm.py"
create_job "${JOB_PREFIX}-gold"   "cde/jobs/build_gold.py"
create_job "${JOB_PREFIX}-recon"  "cde/jobs/reconcile.py"
create_job "${JOB_PREFIX}-mdm-live" "cde/jobs/mdm_live.py"

echo ""
echo "Jobs deployed from ${REPO_NAME}. One stage by hand (run-time args replace the job's):"
echo "  cde job run --name ${JOB_PREFIX}-bronze --arg=--business-date --arg=2026-09-21 \\"
echo "    --arg=--db-prefix --arg=${DB_PREFIX} --arg=--landing --arg=${LANDING} --wait"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"
