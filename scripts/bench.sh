#!/bin/sh
# Time the image on a Docker volume with the scorer's flags: scripts/bench.sh VOLUME [RUNS]
# Output goes to a throwaway volume. Prints wall time per run.
set -eu
IMAGE="${IMAGE:-ss-entry}"; VOL="$1"; RUNS="${2:-3}"
docker volume rm -f ssout >/dev/null 2>&1 || true
docker volume create ssout >/dev/null
MSYS_NO_PATHCONV=1 docker run --rm -v ssout:/o alpine chmod 777 /o
for i in $(seq 1 "$RUNS"); do
  MSYS_NO_PATHCONV=1 docker run --rm -v ssout:/o alpine sh -c 'rm -rf /o/*'
  s=$(date +%s.%N)
  MSYS_NO_PATHCONV=1 docker run --rm --platform linux/amd64 --network none --read-only \
    --tmpfs /tmp:rw,size=64m,mode=1777 --cpus 8 --cpuset-cpus 0-7 --memory 24g --memory-swap 24g \
    --user 1000:1000 -v "$VOL":/data:ro -v ssout:/output --tmpfs /scratch:rw,mode=1777 \
    -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 \
    "$IMAGE" 2>&1 | sed 's/^/    /'
  e=$(date +%s.%N)
  echo "run $i: $(echo "$e - $s" | bc) s"
done
