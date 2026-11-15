# Demo Run Order: who runs what, and when

Go through the stages in order. **Don't start a step until the step before it has passed.**
Commands run on each person's Ubuntu machine/VM, inside a terminal, as your normal user (not root).

| Node | Person | Runs | ZeroTier IP |
|---|---|---|---|
| node1 | Aniket | Ingestion (also backup for node4) | 10.31.88.79 |
| node2 | Aryan | HDFS NameNode + DataNode, Spark worker | 10.31.88.110 |
| node3 | Khush | Spark master + worker, HDFS DataNode | 10.31.88.128 |
| node4 | spare laptop | Search API + UI | 10.31.88.99 |

---

## Stage 0: Get the code (everyone, once)

Check ZeroTier first. It must say `OK` and show your `10.31.88.x` IP:
```bash
sudo zerotier-cli listnetworks
```

**If you do NOT have `~/dse` yet**, clone it:
```bash
git clone https://github.com/Aryan-20-2006/Distributed-search-engine.git ~/dse
cd ~/dse
```

**If you already have `~/dse`**, check that it's the right repo, then update it:
```bash
cd ~/dse
git remote get-url origin     # must print https://github.com/Aryan-20-2006/Distributed-search-engine.git
git pull
```
If it prints a different URL (for example NullPointerX-ui), delete it and clone again:
```bash
rm -rf ~/dse
git clone https://github.com/Aryan-20-2006/Distributed-search-engine.git ~/dse
cd ~/dse
```

Check that you have the real IPs:
```bash
grep ^NODE cluster.env        # must show 10.31.88.79 / .110 / .128 / .99
```

> Never edit files inside `~/dse` on your node. Code changes are pushed to GitHub, and you only ever run `git pull`.

### Dataset (Aniket only)
The dataset is **not** in the repo (Git ignores `*.jsonl`), so cloning won't fetch it. Only **node1 (Aniket)** needs it, kept **outside** the repo folder:
```bash
ls -lh ~/data/pages.jsonl ~/data/changes.jsonl     # both must exist
```
If you saved it inside `~/dse/`, move it out **before** deleting or re-cloning `~/dse`:
```bash
mkdir -p ~/data && mv ~/dse/data/*.jsonl ~/data/
```
(Optional backup: Khush keeps a copy in his `~/data/` too, so node3 can run the ingest if node1 fails.)

---

## Stage 1: Setup (everyone at the same time)

**Aryan (node2):** ✅ already done

**Aniket (node1):**
```bash
cd ~/dse && git pull
sudo bash scripts/setup_hosts.sh 1
sudo WITH_SEARCH=1 bash scripts/install_node.sh 1
source /etc/profile.d/bigdata.sh
python3.10 --version          # must print Python 3.10.x

# check the dataset (files in ~/data/), then send the output to Aryan
python3 scripts/inspect_dataset.py ~/data/pages.jsonl ~/data/changes.jsonl
```

**Khush (node3):**
```bash
cd ~/dse && git pull
sudo bash scripts/setup_hosts.sh 3
sudo bash scripts/install_node.sh 3
source /etc/profile.d/bigdata.sh
```

**node4 (spare laptop, 10.31.88.99):** (Aniket's node1 is the backup if this laptop fails)
```bash
cd ~/dse && git pull
sudo bash scripts/setup_hosts.sh 4
sudo bash scripts/install_node.sh 4
source /etc/profile.d/bigdata.sh
```

`setup_hosts.sh` ends by pinging node1 to node4. Each node should say `reachable`.

> **node4 moved to 10.31.88.99.** If you ran `setup_hosts.sh` before this change, run `cd ~/dse && git pull` and then run `sudo bash scripts/setup_hosts.sh N` again so your `/etc/hosts` points node4 to `.99`.

---

## Stage 2: Start the cluster (in this order)

**1. Aryan (node2):** start HDFS first
```bash
bash scripts/start_node.sh 2
```

**2. Khush (node3):** start the Spark master
```bash
bash scripts/start_node.sh 3
```

**3. Aryan (node2):** check the cluster and create the tables
```bash
bash scripts/check_cluster.sh                # must show 2 DataNodes, 2 workers
bash scripts/submit.sh jobs/init_tables.py   # creates the tables, run ONCE only
```

---

## Stage 3: Phase 0 (base data)

**0. Everyone:** get the latest code fixes before running anything
```bash
cd ~/dse && git pull
```

**1. Aniket (node1):** ingest the base corpus
```bash
bash scripts/node1_ingest_phase.sh 0 ~/data/pages.jsonl
```

**2. Khush (node3):** index, PageRank and export
```bash
bash scripts/node3_process.sh
```

**3. node4 (spare laptop), or Aniket if it fails:** start the search API and load the export
```bash
sudo bash search/install_service.sh
bash search/pull_export.sh
```

**4. Baseline checks.** Write down doc 146's rank, because you'll compare against it after Phase 1:
```bash
# node2
hdfs dfsadmin -report
hdfs dfs -ls -R / | grep -i jsonl
hdfs fsck /input/pages.jsonl -files -blocks -locations

# node4
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool

# node3
spark-sql -e "SELECT doc_id, rank FROM lake.db.pagerank WHERE doc_id = '146';"
```
HDFS web UI: `http://10.31.88.110:9870` → Utilities → Browse the file system → `/input`

> ⚠️ Do NOT run Phase 1 until every Phase 0 check above has passed.

---

## Stage 4: Phase 1 rehearsal (exactly what we'll do live on Monday)

**1. Aniket (node1):**
```bash
bash scripts/node1_ingest_phase.sh 1 ~/data/changes.jsonl
```

**2. Khush (node3):**
```bash
bash scripts/node3_process.sh
```

**3. node4 (spare laptop), or Aniket:**
```bash
bash search/pull_export.sh
```

**4. Checklist §2 to §4:**
```bash
# node3
spark-sql -e "SELECT snapshot_id, committed_at FROM lake.db.pages.snapshots;"
spark-sql -e "SELECT term, doc_id FROM lake.db.postings WHERE doc_id = 'phase1-new-001' LIMIT 5;"
spark-sql -e "SELECT COUNT(*) AS deleted_doc_ranks FROM lake.db.pagerank WHERE doc_id = '1';"
spark-sql -e "SELECT doc_id, title FROM lake.db.pages WHERE doc_id = '94';"
spark-sql -e "SELECT doc_id, rank FROM lake.db.pagerank WHERE doc_id = '146';"
hdfs dfs -ls /serving/v0/
hdfs dfs -ls /serving/v1/
hdfs dfs -test -e /serving/v1/_READY && echo "v1 ready"

# node4
curl -s http://localhost:8000/health
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool
curl -s "http://localhost:8000/search?q=cooking&limit=5" | python3 -m json.tool
curl -s "http://localhost:8000/search?q=plant+growth&limit=5"
```

**5. Idempotency.** Count, re-run the Phase 1 ingest on node1, then count again. The two counts must match:
```bash
# node3
spark-sql -e "SELECT COUNT(*) FROM lake.db.pages;"
# node1
bash scripts/node1_ingest_phase.sh 1 ~/data/changes.jsonl
# node3
spark-sql -e "SELECT COUNT(*) FROM lake.db.pages;"
```

---

## Stage 5: Reset to Phase 0 (tonight, right after the rehearsal)

We do NOT re-run Phase 0. Iceberg kept the Phase 0 table versions, so we just point the tables back at them. Takes ~5 minutes.

**1. Aryan (node2).** Only when no Spark job is running:
```bash
cd ~/dse && git pull
bash scripts/reset_to_phase0.sh
```
Expect at the end:
```
1000
94	Beard
146	0.0017555209979530698
... /serving/v0          (only v0, no v1)
```

**2. node4 (spare laptop):** switch back to v0 and delete the rehearsal copy:
```bash
bash search/pull_export.sh 0
rm -rf /serving/v1          # IMPORTANT: otherwise Monday's pull reuses tonight's rehearsal v1
curl -s http://localhost:8000/health        # {"status":"ok","version":0}
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool   # doc 94 "Beard"
```

After this, **don't run anything else** until Monday. The data stays on disk when laptops are shut down.

---

## Stage 6: Monday, before the demo (start 1 hour early, not minutes before)

**1. Everyone.** Laptops charged and plugged in, sleep turned off. Check ZeroTier, **from the network you'll present on**:
```bash
sudo zerotier-cli listnetworks       # must say OK
ping -c3 node2                       # must reply
```
If the network is slow or blocked, put all 4 laptops on one phone hotspot.

**2. Aryan (node2).** Open the Ubuntu-22.04 window (Windows Terminal → Ubuntu-22.04, or `wsl -d Ubuntu-22.04`) and keep it open:
```bash
cd ~/dse && bash scripts/start_node.sh 2
```

**3. Khush (node3).** Start the VM, then:
```bash
cd ~/dse && bash scripts/start_node.sh 3
stop-history-server.sh               # saves memory on the small VM
```

**4. Aryan (node2).** Check the cluster:
```bash
bash scripts/check_cluster.sh        # Live datanodes (2), 2 ALIVE workers
```

**5. node4 (spare laptop).** The search service starts by itself at boot:
```bash
curl -s http://localhost:8000/health        # {"status":"ok","version":0}
```

**6. Open in a browser** (any laptop on ZeroTier):
- HDFS: `http://10.31.88.110:9870` (Datanodes / Utilities → Browse the file system)
- Search: `http://10.31.88.99:8000`

---

## Stage 7: THE DEMO: run only this (in order)

Everything is already set up (Stage 6). **Start step 1 the moment the slot begins.** Phase 1 takes ~30 min,
so show the Phase 0 checks (step 0) **while** steps 1 to 3 run.
`WARN ... Lost executor` / `Slow waitForAckedSeqno` lines are normal on this network (Spark recovers, which is fault tolerance).
Only stop if a step ends in `Traceback`.

**0. Phase 0 checks (show these while Phase 1 runs):**
```bash
# node2 (Aryan)
hdfs dfsadmin -report                                   # Live datanodes (2)
hdfs dfs -ls -R / | grep -i jsonl                       # /input/pages.jsonl, 9.4 MB
hdfs fsck /input/pages.jsonl -files -blocks -locations  # Live_repl=2, HEALTHY
# Browser: http://10.31.88.110:9870 -> Datanodes, and Utilities -> Browse the file system -> /input
```
Baseline values to quote: doc 94 title = **"Beard"**, doc 146 rank = **0.0017555209979530698**.

**1. Aniket (node1): Phase 1 ingest (~5 min):**
```bash
cd ~/dse
bash scripts/node1_ingest_phase.sh 1 ~/data/changes.jsonl
```
Wait for `DONE. pages snapshot 7157477335341662673 -> <new id>. pages now=975`, then tell Khush.

**2. Khush (node3): index, PageRank (10 iterations) and export (~15-20 min):**
```bash
cd ~/dse
ITERATIONS=10 bash scripts/node3_process.sh
```
Look for: `deleted 50 / updated 25 / inserted 25`, `re-tokenized 50 docs`, `PAGERANK DONE: sum of ranks = 1.000000`,
`EXPORT PUBLISHED: hdfs://node2:9000/serving/v1`. Then tell node4.

**3. node4 (spare laptop): switch the search to v1 (seconds):**
```bash
cd ~/dse
bash search/pull_export.sh                    # >> now serving v1
```

**4. Checklist §2: check each job (node3, one command at a time):**
```bash
spark-sql -e "SELECT snapshot_id, committed_at FROM lake.db.pages.snapshots;"                           # >= 2 snapshots
spark-sql -e "SELECT term, doc_id FROM lake.db.postings WHERE doc_id = 'phase1-new-001' LIMIT 5;"       # 5 rows
spark-sql -e "SELECT COUNT(*) AS deleted_doc_ranks FROM lake.db.pagerank WHERE doc_id = '1';"           # 0
hdfs dfs -ls /serving/v0/                                                                              # docs.parquet, index.parquet
hdfs dfs -ls /serving/v1/                                                                              # docs.parquet, index.parquet
```

**5. Checklist §3: search (node4 curl + node3 spark-sql; these two machines can run at the same time):**
```bash
# node4
curl -s http://localhost:8000/health                                              # {"status":"ok","version":1}
curl -s "http://localhost:8000/search?q=beard&limit=5" | python3 -m json.tool      # doc 94 "Beard (Phase 1)"
curl -s "http://localhost:8000/search?q=cooking&limit=5" | python3 -m json.tool    # doc 146 in results
curl -s "http://localhost:8000/search?q=plant+growth&limit=5"                      # valid JSON, no 500

# node3
hdfs dfs -test -e /serving/v1/_READY && echo "v1 ready"                             # v1 ready
spark-sql -e "SELECT doc_id, title FROM lake.db.pages WHERE doc_id = '94';"        # 94  Beard (Phase 1)
spark-sql -e "SELECT doc_id, rank FROM lake.db.pagerank WHERE doc_id = '146';"     # higher than 0.0017555209979530698
```
Browser: `http://10.31.88.99:8000` now says **"Serving index v1"**.

**6. Checklist §4: idempotency (count, re-run ingest, count again; the counts must match):**
```bash
# node3
spark-sql -e "SELECT COUNT(*) FROM lake.db.pages;"                # 975
# node1 (Aniket)
bash scripts/node1_ingest_phase.sh 1 ~/data/changes.jsonl         # actions={'noop': ...}, "No page changes -> skipping MERGE"
# node3
spark-sql -e "SELECT COUNT(*) FROM lake.db.pages;"                # 975 again
```

---

## If something fails
Send the last 20 lines of the error to Aryan. Quick checks:
```bash
sudo zerotier-cli listnetworks   # must say OK
ping -c2 node2                   # must reply
jps                              # node2: NameNode DataNode Worker / node3: Master Worker DataNode
```
