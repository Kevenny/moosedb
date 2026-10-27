#!/usr/bin/env bash
# Simulated power loss (LazyFS) against MooseDB. See README.md in this directory.
#
#   docs/docker/powerloss/run.sh -n 30 -s 1 -o /path/outside/the/repo [-c 4] [--sync-mode write]
#
# Needs build/ha_moosedb.so (docs/docker/build-plugin.sh). Builds the
# moosedb-powerloss image if missing. FUSE needs --device /dev/fuse and
# --cap-add SYS_ADMIN (PL_PRIVILEGED=1 uses --privileged instead).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash: keep container paths as they are

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && (pwd -W 2>/dev/null || pwd))
IMAGE=${PL_IMAGE:-moosedb-powerloss}
OUT=""
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    -o|--out) OUT=$2; shift 2 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[ -n "$OUT" ] || { echo "usage: $0 -o OUTDIR [-n cycles] [-s seed] [-c clients] [--sync-mode fsync|write]" >&2; exit 2; }
mkdir -p "$OUT"
OUT=$(cd "$OUT" && (pwd -W 2>/dev/null || pwd))
[ -f "$HERE/../../../build/ha_moosedb.so" ] || { echo "build/ha_moosedb.so missing (docs/docker/build-plugin.sh)" >&2; exit 2; }

docker image inspect "$IMAGE" >/dev/null 2>&1 \
  || docker build -f "$HERE/Dockerfile" -t "$IMAGE" "$HERE"

if [ "${PL_PRIVILEGED:-0}" = 1 ]; then
  CAPS=(--privileged)
else
  CAPS=(--device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined)
fi

exec docker run --rm "${CAPS[@]}" -v "$REPO:/repo:ro" -v "$OUT:/out" "$IMAGE" \
  python3 /repo/docs/docker/powerloss/powerloss.py -o /out "${ARGS[@]}"
