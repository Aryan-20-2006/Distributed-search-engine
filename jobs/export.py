"""Node 3 — Validate and export the serving snapshot as plain Parquet.

    bash scripts/submit.sh jobs/export.py

Writes to hdfs://node2:9000/serving/.tmp_vN/ then, ONLY if every check passes,
atomically renames it to /serving/vN/ (an HDFS rename is atomic) and finally
creates /serving/vN/_READY. Node 4 only pulls versions that have _READY, so it
can never see a half-written or failed export. Previous versions are never touched.
Versions start at v0 (Phase 0), then v1 (Phase 1), ...

Layout of /serving/vN/ (plain single Parquet files):
    docs.parquet   doc_id, url, title, version, doc_len, pagerank
    index.parquet  inverted index: term, doc_id, tf
    manifest.json  counts, avgdl, Iceberg snapshot ids, created_at
    _READY         empty marker, written last
"""
import json
import re
import time

from pyspark.sql import functions as F


def hfs(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p, jvm


def next_version(spark, root):
    """v0 for the first export, then max+1."""
    fs, p, _ = hfs(spark, root)
    if not fs.exists(p):
        fs.mkdirs(p)
        return 0
    nums = [int(m.group(1)) for st in fs.listStatus(p)
            if (m := re.fullmatch(r"v(\d+)", st.getPath().getName()))]
    return max(nums) + 1 if nums else 0


def write_single_parquet(spark, df, tmp_dir, target_file):
    """Write df as ONE plain Parquet file at target_file (Spark always writes a folder
    of part files, so write to a scratch folder and move the single part file out)."""
    fs, _, jvm = hfs(spark, tmp_dir)
    Path = jvm.org.apache.hadoop.fs.Path
    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
    parts = [st.getPath() for st in fs.listStatus(Path(tmp_dir))
             if st.getPath().getName().startswith("part-")]
    if len(parts) != 1:
        raise SystemExit(f"expected 1 part file in {tmp_dir}, found {len(parts)}")
    if not fs.rename(parts[0], Path(target_file)):
        raise SystemExit(f"could not move {parts[0]} -> {target_file}")
    fs.delete(Path(tmp_dir), True)


def write_text(spark, path, text):
    fs, p, _ = hfs(spark, path)
    out = fs.create(p, True)
    out.write(bytearray(text.encode("utf-8")))
    out.close()


def validate(docs, postings, n_pages):
    """Returns (stats dict, list of failure strings)."""
    errors = []
    n_docs = docs.count()
    n_post = postings.count()
    if n_docs == 0:
        errors.append("docs is empty")
    if n_docs != n_pages:
        errors.append(f"docs ({n_docs}) != pages ({n_pages})")
    if n_post == 0:
        errors.append("postings is empty")
    nulls = docs.filter(F.col("url").isNull() | F.col("pagerank").isNull() |
                        F.col("doc_len").isNull()).count()
    if nulls:
        errors.append(f"{nulls} docs with null url/pagerank/doc_len")
    if docs.groupBy("doc_id").count().filter("count > 1").limit(1).count():
        errors.append("duplicate doc_id in docs")
    orphans = postings.join(docs, "doc_id", "left_anti").count()
    if orphans:
        errors.append(f"{orphans} postings point to unknown doc_ids")
    pr_sum = docs.agg(F.sum("pagerank")).first()[0] or 0.0
    if abs(pr_sum - 1.0) > 1e-3:
        errors.append(f"pagerank sums to {pr_sum:.6f}, expected 1.0")
    avgdl = docs.agg(F.avg("doc_len")).first()[0] or 0.0
    n_terms = postings.select("term").distinct().count()
    return {"num_docs": n_docs, "num_postings": n_post, "num_terms": n_terms,
            "avgdl": float(avgdl), "pagerank_sum": float(pr_sum)}, errors


def main():
    from common import (EXPORT_ROOT, T_DOC_STATS, T_LINKS, T_PAGERANK, T_PAGES, T_POSTINGS,
                        banner, current_snapshot, get_spark, get_state, state_snapshots, timed)

    spark = get_spark("export-serving-parquet")
    snaps = {t.split(".")[-1]: current_snapshot(spark, t)
             for t in (T_PAGES, T_LINKS, T_POSTINGS, T_DOC_STATS, T_PAGERANK)}
    idx_state = state_snapshots(get_state(spark, "index"))
    pr_state = state_snapshots(get_state(spark, "pagerank"))

    errors = []
    # Consistency: index + pagerank must have been computed from the CURRENT data.
    if idx_state.get("pages_snapshot") != snaps["pages"]:
        errors.append(f"index is stale (built from pages {idx_state.get('pages_snapshot')}, "
                      f"current {snaps['pages']}) -> run index.py first")
    if pr_state.get("links_snapshot") != snaps["links"] or pr_state.get("pages_snapshot") != snaps["pages"]:
        errors.append("pagerank is stale -> run pagerank.py first")

    pages = spark.table(T_PAGES)
    docs = (pages.select("doc_id", "url", "title", "version")
            .join(spark.table(T_DOC_STATS), "doc_id", "left")
            .join(spark.table(T_PAGERANK).withColumnRenamed("rank", "pagerank"), "doc_id", "left")
            .cache())
    postings = spark.table(T_POSTINGS).select("term", "doc_id", "tf").cache()

    with timed("validation"):
        stats, v_errors = validate(docs, postings, pages.count())
    errors += v_errors
    print(json.dumps(stats, indent=2))
    if errors:
        banner("EXPORT REJECTED - nothing published, previous version stays live:\n  - "
               + "\n  - ".join(errors))
        spark.stop()
        raise SystemExit(1)

    version = next_version(spark, EXPORT_ROOT)
    tmp, final = f"{EXPORT_ROOT}/.tmp_v{version}", f"{EXPORT_ROOT}/v{version}"
    fs, _, jvm = hfs(spark, tmp)
    fs.delete(jvm.org.apache.hadoop.fs.Path(tmp), True)

    with timed(f"write parquet to {tmp}"):
        write_single_parquet(spark, docs.orderBy("doc_id"), f"{tmp}/_docs_parts", f"{tmp}/docs.parquet")
        write_single_parquet(spark, postings.orderBy("term", "doc_id"),
                             f"{tmp}/_index_parts", f"{tmp}/index.parquet")
        # read back what was written and re-check counts (catches partial writes)
        rd = spark.read.parquet(f"{tmp}/docs.parquet").count()
        rp = spark.read.parquet(f"{tmp}/index.parquet").count()
        if (rd, rp) != (stats["num_docs"], stats["num_postings"]):
            fs.delete(jvm.org.apache.hadoop.fs.Path(tmp), True)
            raise SystemExit(f"read-back mismatch docs={rd} postings={rp}; export aborted")

    manifest = {"version": version, "status": "complete",
                "files": {"docs": "docs.parquet", "index": "index.parquet"},
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "iceberg_snapshots": snaps, "pagerank_iterations": pr_state.get("iterations"),
                **stats}
    write_text(spark, f"{tmp}/manifest.json", json.dumps(manifest, indent=2))
    if not fs.rename(jvm.org.apache.hadoop.fs.Path(tmp), jvm.org.apache.hadoop.fs.Path(final)):
        raise SystemExit("HDFS rename failed")
    write_text(spark, f"{final}/_READY", "")      # marker LAST: vN is complete and servable
    banner(f"EXPORT PUBLISHED: {final}\n{json.dumps(manifest, indent=2)}")
    spark.stop()


if __name__ == "__main__":
    main()
