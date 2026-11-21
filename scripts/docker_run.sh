#!/bin/sh
# Run the image with the scorer's flags: scripts/docker_run.sh DATA_DIR OUTPUT_DIR
# (Adapted from the starter's script so it also works from Git Bash on Windows.)
set -eu
IMAGE="${IMAGE:-ss-entry}"
CPUS="${CPUS:-8}"
MEM="${MEM:-24g}"
abs() { (cd "$1" && (pwd -W 2>/dev/null || pwd)); }
DATA=$(abs "$1")
mkdir -p "$2"
OUT=$(abs "$2")
SCRATCH_HOST=$(mktemp -d)
SCRATCH=$(abs "$SCRATCH_HOST")
chmod 777 "$2" "$SCRATCH_HOST"
trap 'rm -rf "$SCRATCH_HOST" 2>/dev/null || true' EXIT
MSYS_NO_PATHCONV=1 docker run --rm --platform linux/amd64 \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$DATA":/data:ro -v "$OUT":/output -v "$SCRATCH":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" \
  "$IMAGE"
