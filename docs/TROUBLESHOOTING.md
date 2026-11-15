# Troubleshooting notes: setup day (2026-11-15)

Everything that went wrong while setting up the cluster, why, and the fix. Read this first if something breaks on demo day.
The run order is in [DEMO_STEPS.md](../DEMO_STEPS.md) (Monday = Stage 6, then Stage 7).

## Cluster facts

| Node | Who / machine | ZeroTier IP | Runs |
|---|---|---|---|
| node1 | Aniket (VM, user `pes2ug24cs063`) | 10.31.88.79 | Ingestion driver; backup search API (`WITH_SEARCH=1`) |
| node2 | Aryan, **WSL Ubuntu-22.04** on Windows (user `ubuntu`) | 10.31.88.110 | HDFS NameNode + DataNode, Spark worker |
| node3 | Khush, **Multipass/cloud-init VM on Mac** (user `ubuntu`, 1.9 GB RAM + 2 GB swap) | 10.31.88.128 | Spark master + worker, DataNode |
| node4 | Spare laptop (user `khush`) | 10.31.88.99 | Search API (systemd service `search`, starts at boot) |

- ZeroTier network ID: `a581878f7d2d3958`, subnet `10.31.88.0/24`. Aryan's Windows host is `.84` (not a cluster node).
- Repo everyone uses: `https://github.com/Aryan-20-2006/Distributed-search-engine.git` (a fork of NullPointerX-ui's; don't use that one).
- Web UIs: HDFS `http://10.31.88.110:9870` (also `http://localhost:9870` on Aryan's Windows), Spark master `http://10.31.88.128:8080`, search `http://10.31.88.99:8000`.

## Phase 0 baseline (after `reset_to_phase0.sh`)

- pages = **1000** (the dataset has 1040 lines: 20 deliberately malformed, rejected on purpose)
- doc 94 title = **"Beard"**, doc 146 rank = **0.0017555209979530698**
- HDFS `/serving/v0` = docs.parquet, index.parquet, manifest.json, _READY
- Phase 1 rehearsal produced: deleted 50 / updated 25 / inserted 25 → **975** pages; idempotent re-run = all `noop`

---

## Problems we hit and their fixes

### Network / ZeroTier
| Symptom | Cause | Fix |
|---|---|---|
| `ACCESS_DENIED` in `zerotier-cli listnetworks` | Member not authorized | Tick **Authorized** on my.zerotier.com |
| A node shows no IP on my.zerotier.com | WSL/VM was stopped | Start it; the IP appears within seconds |
| `ping node2: Temporary failure in name resolution` | `/etc/hosts` missing node entries (old/non-git copy of the repo) | `git pull`, then `sudo bash scripts/setup_hosts.sh N` |
| `cluster.env` shows `10.31.88.___` | The folder was a ZIP download, not a clone | `mv ~/dse ~/dse.old`, clone again, re-run setup_hosts + install_node |
| Slow jobs, `RELAY` in `sudo zerotier-cli peers` | Wi-Fi blocks direct ZeroTier connections | All laptops on **one phone hotspot**; connect **before** starting services |
| node1 can't ping node4 | Peers not directly connected yet | Harmless: node1 never talks to node4 |

### Install
| Symptom | Cause | Fix (already in the scripts) |
|---|---|---|
| `ERROR: Could not find a profile matching 'OpenSSH'` | Desktop Ubuntu has no SSH server | Script now uses `ufw allow 22/tcp`. `git pull` and re-run install |
| `Running Spark version 3.3.4` + `NoSuchMethodException: java.nio.DirectByteBuffer` | Old Spark left in `/opt/spark` + Java 21 default (Aniket's VM) | Script moves wrong versions aside and pins Java 11. `git pull`, `sudo WITH_SEARCH=1 bash scripts/install_node.sh 1` |
| Python version mismatch driver vs executors | Ubuntu 20.04/24.04 ship a different Python | Script pins python3.10 on every node |
| `hdfs`/`spark-sql: command not found` in a new terminal | Env not loaded | `echo 'source /etc/profile.d/bigdata.sh' >> ~/.bashrc && source ~/.bashrc` |
| Hostname wrong (two machines called node3) | `setup_hosts.sh` run with the wrong N | Re-run with the right N; check with `hostname` |

### Running the cluster
| Symptom | Cause | Fix |
|---|---|---|
| node2's `jps` empty, NameNode log `RECEIVED SIGNAL 1: SIGHUP` | Daemons died when the terminal closed | `start_node.sh` now detaches (setsid). Restart: `bash scripts/start_node.sh 2` |
| `Dead datanodes (1): node3`; ingest stuck on `Waiting for /input/... .....` | node3 VM out of memory (1.9 GB, no swap) | Done on node3: 2 GB swap + `SPARK_WORKER_MEMORY=1g` + `stop-history-server.sh`. Then `stop_node.sh 3 && start_node.sh 3` |
| Worker missing at `:8080` after restarts | Worker started before the master | On that node: `stop-worker.sh; setsid start-worker.sh spark://node3:7077 </dev/null >/dev/null 2>&1` (**only when no job is running**) |
| `Lost executor ... worker lost: Not receiving heartbeat for 60 seconds`, `Slow waitForAckedSeqno`, `SocketTimeoutException 60000 millis` | Slow relayed network | **Leave it running.** Spark recovers (fault tolerance). Only act if it ends in `Traceback` (then re-run the same command) |

### Pipeline
| Symptom | Cause | Fix |
|---|---|---|
| Ingest: `rejected=1040`, `missing_or_invalid_version` | Course dataset has **no version field** | Fixed: a missing version = the phase number (0 or 1) |
| `pull_export.sh`: `RELOAD FAILED`, HTTP 500 | Search service still running old code | `sudo systemctl restart search`, then `bash search/pull_export.sh 0` (or no arg for newest) |
| Website shows the wrong version | Wrong export active | `bash search/pull_export.sh <N>`; check `curl -s localhost:8000/health` |
| Phase 1 ingest/index start before the previous step finished | Ordering | Each step waits for the previous `DONE` / `EXPORT PUBLISHED` line |

---

## Emergency recipes

**Back to the Phase 0 baseline** (e.g. a failed live run; takes ~5 min, no recompute):
```bash
# node2, when no Spark job is running
cd ~/dse && bash scripts/reset_to_phase0.sh
# node4
bash search/pull_export.sh 0 && rm -rf /serving/v1
```

**A Phase 1 step failed halfway:** just re-run the same step. Ingest is idempotent, index is incremental
(says "already up to date" if done), and export only publishes when everything passes.

**node2 (WSL) went down** (laptop slept or WSL closed): open Ubuntu-22.04, then `cd ~/dse && bash scripts/start_node.sh 2`.
If node3's worker or DataNode then looks dead, Khush runs `bash scripts/stop_node.sh 3 && bash scripts/start_node.sh 3`.

**Quick health check (node2):**
```bash
bash scripts/check_cluster.sh       # Live datanodes (2), 2 ALIVE workers
sudo zerotier-cli peers             # DIRECT is good, RELAY is slow
```

**Plan B if node4 (spare laptop) fails:** Aniket's node1 has the search venv. On node1:
`sudo bash search/install_service.sh && bash search/pull_export.sh`, then run the node4 `curl` checks there.
