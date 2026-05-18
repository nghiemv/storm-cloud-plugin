#!/usr/bin/env bash
# batch_run.sh — Stage manifests and run storm-cloud-plugin for multiple runs sequentially.
#
# What this does:
#   1. Copies each compute-manifest.json to the CC SDK's expected path in S3:
#        s3://$BUCKET/manifests/<uuid>/payload
#   2. Runs the container once per manifest, sequentially.
#
# Outputs land at s3://$BUCKET/<run-name>/ (defined in each manifest's output_path).
#
# Required env (set in a private .env file, sourced into the shell):
#   BATCH_AWS_ACCESS_KEY_ID
#   BATCH_AWS_SECRET_ACCESS_KEY
#   BATCH_AWS_ENDPOINT          (e.g. https://s3.hecdev.net)
#   BATCH_AWS_REGION            (default: us-east-1)
#   BATCH_AWS_S3_BUCKET         (default: storm-cloud)
#
# Optional:
#   AORC_CACHE                  (set to s3://<bucket>/<prefix> once mirrored to skip NOAA)
#   IMAGE                       (default: storm-cloud-plugin:latest)

set -euo pipefail

: "${BATCH_AWS_ACCESS_KEY_ID:?must be set}"
: "${BATCH_AWS_SECRET_ACCESS_KEY:?must be set}"
: "${BATCH_AWS_ENDPOINT:?must be set}"

REGION="${BATCH_AWS_REGION:-us-east-1}"
BUCKET="${BATCH_AWS_S3_BUCKET:-storm-cloud}"
IMAGE="${IMAGE:-storm-cloud-plugin:latest}"
AORC_CACHE="${AORC_CACHE:-}"

SC_ROOT="manifests"
LOCAL_OUTPUTS="$(cd "$(dirname "$0")/.." && pwd)/outputs"

# --- Run definitions: "run-name|uuid-from-input.json" ---
RUNS=(
  "indian-creek-72hr|1a632671-97ff-4dc4-81c8-514774a04efc"
  "kanawha-1000-120hr|63c1c4b9-88f9-4187-a093-0ae38008fbc0"
  "kanawha-1000-48hr|68e0442a-da10-4189-8aa0-a9d7e249b424"
)

# ── Stage manifests ──────────────────────────────────────────────────────────
echo "=== Staging manifests to s3://$BUCKET/$SC_ROOT/<uuid>/payload ==="

mc alias set hecdev "$BATCH_AWS_ENDPOINT" \
  "$BATCH_AWS_ACCESS_KEY_ID" "$BATCH_AWS_SECRET_ACCESS_KEY" \
  --api S3v4 >/dev/null 2>&1

for entry in "${RUNS[@]}"; do
  name="${entry%%|*}"
  uuid="${entry##*|}"
  echo "  $name  ->  $SC_ROOT/$uuid/payload"
  mc cp \
    "hecdev/$BUCKET/$name/compute-manifest.json" \
    "hecdev/$BUCKET/$SC_ROOT/$uuid/payload"
done

# ── Run each manifest ────────────────────────────────────────────────────────
AORC_OPTS=()
if [[ -n "$AORC_CACHE" ]]; then
  AORC_OPTS=(
    -e AORC_S3_BASE_URL="$AORC_CACHE"
    -e AORC_S3_KEY="$BATCH_AWS_ACCESS_KEY_ID"
    -e AORC_S3_SECRET="$BATCH_AWS_SECRET_ACCESS_KEY"
    -e AORC_S3_ENDPOINT="$BATCH_AWS_ENDPOINT"
  )
fi

for entry in "${RUNS[@]}"; do
  name="${entry%%|*}"
  uuid="${entry##*|}"

  echo ""
  echo "=== Starting: $name (payload=$uuid) ==="

  mkdir -p "$LOCAL_OUTPUTS/$name"

  docker run --rm \
    -v "$LOCAL_OUTPUTS/$name:/usr/src/app/Local" \
    -e CC_MANIFEST_ID="$uuid" \
    -e CC_PAYLOAD_ID="$uuid" \
    -e CC_ROOT="$SC_ROOT" \
    -e CC_AWS_ACCESS_KEY_ID="$BATCH_AWS_ACCESS_KEY_ID" \
    -e CC_AWS_SECRET_ACCESS_KEY="$BATCH_AWS_SECRET_ACCESS_KEY" \
    -e CC_AWS_DEFAULT_REGION="$REGION" \
    -e CC_AWS_ENDPOINT="$BATCH_AWS_ENDPOINT" \
    -e CC_AWS_S3_BUCKET="$BUCKET" \
    -e FFRD_AWS_ACCESS_KEY_ID="$BATCH_AWS_ACCESS_KEY_ID" \
    -e FFRD_AWS_SECRET_ACCESS_KEY="$BATCH_AWS_SECRET_ACCESS_KEY" \
    -e FFRD_AWS_DEFAULT_REGION="$REGION" \
    -e FFRD_AWS_ENDPOINT="$BATCH_AWS_ENDPOINT" \
    -e FFRD_AWS_S3_BUCKET="$BUCKET" \
    "${AORC_OPTS[@]}" \
    "$IMAGE"

  echo "=== Done: $name ==="
done

echo ""
echo "All runs complete."
