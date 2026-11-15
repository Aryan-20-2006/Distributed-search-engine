#!/usr/bin/env bash
# Run on NODE 2 (or node3), AFTER the Phase 1 rehearsal, when NO Spark job is running.
#   bash scripts/reset_to_phase0.sh
# Puts the lakehouse back to the exact Phase 0 state (Iceberg snapshots recorded on
# 2026-11-15 right after Phase 0) and deletes the rehearsal export /serving/v1+, so
# Phase 1 can be run live at the demo. Nothing is recomputed.
# Afterwards on node4:  bash search/pull_export.sh 0 && rm -rf /serving/v1
set -euo pipefail
source /etc/profile.d/bigdata.sh

# lake.db.<table> -> its snapshot right after Phase 0
declare -A SNAP=(
  [pages]=7157477335341662673
  [links]=6333501676682979130
  [rejects]=7879036908732739077
  [ingest_log]=290884674276661528
  [postings]=4608924612145844559
  [doc_stats]=9016696337279920685
  [pagerank]=9065974735795352674
  [job_state]=1407954743100718643
)

# 1. Safety: refuse while a Spark application is running.
ACTIVE="$(curl -s -m 15 http://node3:8080/json/ | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["activeapps"]))' || echo "?")"
if [ "$ACTIVE" != "0" ]; then
  echo "A Spark job is running (or the master is unreachable): active apps = $ACTIVE. Wait for it to finish."; exit 1
fi

# 2. Point every table back at its Phase 0 snapshot (old snapshots are kept by Iceberg).
SQL=""
for t in "${!SNAP[@]}"; do
  SQL+="CALL lake.system.set_current_snapshot('db.$t', ${SNAP[$t]}); "
done
echo ">> restoring tables to their Phase 0 snapshots"
spark-sql -S -e "$SQL" 2>&1 | grep -vE "^(WARN|INFO)|^[0-9]{2}/[0-9]{2}/[0-9]{2} " || true

# 3. Remove rehearsal exports (everything above v0).
for d in $(hdfs dfs -ls /serving 2>/dev/null | awk '{print $NF}' | grep -E '/serving/v[0-9]+$' || true); do
  [ "$(basename "$d")" = "v0" ] && continue
  echo ">> removing rehearsal export $d"; hdfs dfs -rm -r -f -skipTrash "$d" >/dev/null
done
hdfs dfs -rm -r -f -skipTrash '/serving/.tmp_v*' >/dev/null 2>&1 || true

# 4. Verify the baseline.
echo ">> verification (expect: 1000 | 94 Beard | 146 0.0017555209979530698 | /serving/v0 only)"
spark-sql -S -e "SELECT COUNT(*) AS pages FROM lake.db.pages;
                 SELECT doc_id, title FROM lake.db.pages WHERE doc_id = '94';
                 SELECT doc_id, rank FROM lake.db.pagerank WHERE doc_id = '146';" 2>/dev/null
hdfs dfs -ls /serving
echo ">> done. Now on node4:  bash search/pull_export.sh 0 && rm -rf /serving/v1"
