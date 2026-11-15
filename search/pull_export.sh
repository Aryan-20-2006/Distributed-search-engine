#!/usr/bin/env bash
# Run on NODE 4.  bash search/pull_export.sh          -> pull the newest _READY /serving/vN from HDFS
#                 bash search/pull_export.sh 1        -> (re)activate a specific version, e.g. v1
# Steps: download to /serving/.incoming_vN -> verify -> rename to /serving/vN ->
#        flip the /serving/current symlink atomically -> ask the API to reload.
# If verify or reload fails, the previous version keeps serving (symlink is reverted).
set -euo pipefail
source /etc/profile.d/bigdata.sh
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="${SERVING_ROOT:-/serving}"; API="${API:-http://localhost:8000}"
PY=/opt/searchenv/bin/python

HROOT="${HDFS_SERVING:-/serving}"     # exports on HDFS: $HROOT/vN/{docs.parquet,index.parquet,manifest.json,_READY}

if [ -n "${1:-}" ]; then V="v$1"
else
  # newest vN on HDFS that has its _READY marker
  V=""
  for cand in $(hdfs dfs -ls "$HROOT" 2>/dev/null | awk '{print $NF}' | grep -E "/v[0-9]+$" | xargs -rn1 basename | sort -rV); do
    if hdfs dfs -test -e "$HROOT/$cand/_READY"; then V="$cand"; break; fi
  done
fi
[ -n "$V" ] || { echo "No completed (_READY) exports in HDFS $HROOT yet"; exit 1; }
echo ">> target version: $V"

if [ ! -d "$ROOT/$V" ]; then
  hdfs dfs -test -e "$HROOT/$V/_READY" || { echo "$HROOT/$V has no _READY marker - not pulling"; exit 1; }
  rm -rf "$ROOT/.incoming_$V"
  hdfs dfs -get "$HROOT/$V" "$ROOT/.incoming_$V"
  $PY "$HERE/verify_export.py" "$ROOT/.incoming_$V" || { echo "VERIFY FAILED - keeping current version"; rm -rf "$ROOT/.incoming_$V"; exit 1; }
  mv "$ROOT/.incoming_$V" "$ROOT/$V"
else
  $PY "$HERE/verify_export.py" "$ROOT/$V"
fi

OLD="$(readlink "$ROOT/current" 2>/dev/null || true)"
ln -sfn "$V" "$ROOT/current.new" && mv -T "$ROOT/current.new" "$ROOT/current"
echo ">> /serving/current -> $V (was: ${OLD:-none})"

if curl -fsS -X POST "$API/admin/reload"; then echo; echo ">> now serving $V"
else
  echo ">> RELOAD FAILED - reverting symlink to ${OLD:-none}"
  if [ -n "$OLD" ]; then ln -sfn "$OLD" "$ROOT/current.new" && mv -T "$ROOT/current.new" "$ROOT/current"; fi
  exit 1
fi
ls -l "$ROOT"
