import psycopg2
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── CONFIGURATION ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "HOST.NAME",
    "port":     5432,
    "dbname":   "DATABASE",
    "user":     "USER",
    "password": "SECRET"
}

SHARDS                  = 8           # number of parallel shards/sessions
BATCH_SIZE              = 10000       # rows per batch per shard
USE_MD5                 = True        # True = faster hashing (if crypto strength not required)
COLLAPSE_WHITESPACE     = True        # collapse whitespace before hashing (stabilizes diffs)
SLEEP_BETWEEN_BATCHES   = 0.0         # seconds; throttle per shard if desired
SET_WORK_MEM            = "4GB"       # per session; tune as needed or set to None
SET_PARALLEL_PER_GATHER = 4           # per session; None to skip

RUN_VACUUM_ANALYZE_END  = True        # run a VACUUM ANALYZE after all shards done
LOG_FILENAME            = "refresh_profile_digest_parallel.log"

# ─── LOGGING ───────────────────────────────────────────────────
logger = logging.getLogger("digest_refresher")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)
fh = logging.FileHandler(LOG_FILENAME); fh.setFormatter(fmt); logger.addHandler(fh)

def run_shard(shard_index: int) -> tuple[int, int, float]:
    """Process one shard in a dedicated connection. Returns (processed_total, changed_total, seconds)."""
    t0 = time.time()
    processed_total = 0
    changed_total   = 0

    SQL_BATCH = """
    WITH run AS (
      SELECT *
      FROM profiler.refresh_metadata_profile_digest_batch_shard(
        p_only_ids          => NULL,
        p_batch_size        => %(batch_size)s,
        p_use_md5           => %(use_md5)s,
        p_collapse_ws       => %(collapse_ws)s,
        p_shard_index       => %(shard_index)s,
        p_shard_count       => %(shard_count)s,
        p_run_started_at    => %(run_started_at)s
      )
    )
    SELECT COALESCE(COUNT(*), 0)              AS processed,
           COALESCE(SUM((changed)::int), 0)   AS changed
    FROM run;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        batch_no = 0
        while True:
            batch_no += 1
            with conn:
                with conn.cursor() as cur:
                    # --- per-batch safety knobs (transaction-scoped) ---
                    cur.execute("SET LOCAL synchronous_commit = OFF;")
                    cur.execute("SET LOCAL statement_timeout = %s;", ('15min',))
                    cur.execute("SET LOCAL lock_timeout = %s;", ('5s',))
                    cur.execute("SET LOCAL idle_in_transaction_session_timeout = %s;", ('5min',))
                    cur.execute("SELECT now()")
                    RUN_STARTED_AT = cur.fetchone()[0]
                    if SET_WORK_MEM:
                        cur.execute("SET LOCAL work_mem = %s;", (SET_WORK_MEM,))
                    if SET_PARALLEL_PER_GATHER is not None:
                        cur.execute(
                            "SET LOCAL max_parallel_workers_per_gather = %s;",
                            (SET_PARALLEL_PER_GATHER,)
                        )

                    # --- do one batch for this shard ---
                    cur.execute(SQL_BATCH, {
                        "batch_size":       BATCH_SIZE,
                        "use_md5":          USE_MD5,
                        "collapse_ws":      COLLAPSE_WHITESPACE,
                        "shard_index":      shard_index,
                        "shard_count":      SHARDS,
                        "run_started_at":   RUN_STARTED_AT
                    })
                    row = cur.fetchone()
                    if row is None:
                        # Defensive: should never happen because SQL_BATCH always SELECTs 1 row
                        processed = changed = 0
                    else:
                        processed, changed = row

            processed_total += processed
            changed_total   += changed

            logger.info(
                f"[shard {shard_index}] batch {batch_no}: "
                f"processed={processed:,} changed={changed:,} "
                f"(cum processed={processed_total:,})"
            )

            if processed == 0:
                break

            if SLEEP_BETWEEN_BATCHES:
                time.sleep(SLEEP_BETWEEN_BATCHES)

    finally:
        conn.close()

    return processed_total, changed_total, time.time() - t0

def main():
    logger.info("=== Refresh run started ===")
    t0 = time.time()

    totals_processed = 0
    totals_changed   = 0

    with ThreadPoolExecutor(max_workers=SHARDS) as ex:
        futures = {ex.submit(run_shard, i): i for i in range(SHARDS)}
        for fut in as_completed(futures):
            shard = futures[fut]
            try:
                processed, changed, secs = fut.result()
                totals_processed += processed
                totals_changed   += changed
                logger.info(f"[shard {shard}] DONE: processed={processed:,}, changed={changed:,} "
                            f"in {secs:,.1f}s")
            except Exception as e:
                logger.exception(f"[shard {shard}] FAILED: {e}")

    dt = time.time() - t0
    logger.info(f"=== All shards complete: processed={totals_processed:,}, changed={totals_changed:,}, "
                f"elapsed={dt:,.1f}s ===")

    if RUN_VACUUM_ANALYZE_END:
        logger.info("*** VACUUM ANALYZE profiler.metadata_profile_digest ***")
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("VACUUM ANALYZE profiler.metadata_profile_digest;")
            conn.close()
        except Exception as e:
            logger.exception(f"VACUUM failed: {e}")

    logger.info("=== Refresh run finished ===")

if __name__ == "__main__":
    main()
