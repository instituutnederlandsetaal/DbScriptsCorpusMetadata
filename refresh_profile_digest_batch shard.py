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

# ──────────────────────────────────────────────────────────────────────────────
# Runtime knobs. These control *how* work is split and how aggressively each
# shard session pushes work into Postgres. Tuning them is the fastest way to
# trade latency vs. load.
# ──────────────────────────────────────────────────────────────────────────────
SHARDS                  = 8           # Number of parallel worker threads/sessions.
                                      # Each thread opens its own DB connection and processes
                                      # its shard (pkid % SHARDS == shard_index) in batches.

BATCH_SIZE              = 10000       # Rows per batch per shard. Larger = fewer round trips,
                                      # but bigger transactions/WAL bursts and more memory/live locks.

USE_MD5                 = True        # Use MD5 instead of SHA-256 for the digest. Much faster.

COLLAPSE_WHITESPACE     = True        # Normalize XML string by collapsing whitespace before hashing.

SLEEP_BETWEEN_BATCHES   = 0.0         # Optional pause between batches per shard. Useful to smooth load.

SET_WORK_MEM            = "1GB"       # Per-session work_mem (applies to sorts/hash ops in that session).

SET_PARALLEL_PER_GATHER = 4           # Parallelism within a single query plan node. Sharding already
                                      # gives us parallelism; this rarely helps a lot. None to skip.

RUN_VACUUM_ANALYZE_END  = True        # Light maintenance at the end to keep stats fresh.

LOG_FILENAME            = "refresh_profile_digest_parallel.log"

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logger = logging.getLogger("digest_refresher")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); logger.addHandler(sh)
fh = logging.FileHandler(LOG_FILENAME); fh.setFormatter(fmt); logger.addHandler(fh)


def run_shard(shard_index: int, run_started_at):
    """
    Process one shard (identified by shard_index in [0..SHARDS-1]) until that shard
    reports 0 rows processed (meaning: no candidates left for this shard).

    Returns:
        (processed_total, changed_total, elapsed_seconds)

    Key ideas:
    - We pass the same `run_started_at` (taken from DB clock) to every shard and batch.
      The server-side function stamps `last_checked_at` and only considers rows whose
      last_checked_at < run_started_at for this whole run. That guarantees we don’t
      loop forever on rows we just checked a second ago.
    - Each loop iteration is a single transaction (“one batch”). If the batch SELECT
      returns processed=0, we’re done for this shard.
    """
    t0 = time.time()
    processed_total = 0
    changed_total   = 0

    # The SQL does exactly one “batch” for this shard:
    #   - Calls the server function with batch size, sharding params, and run_started_at.
    #   - Aggregates how many rows were processed and how many changed.

    SQL_BATCH = """
    WITH run AS (
      SELECT *
      FROM profiler.refresh_metadata_profile_digest_batch_shard(
        p_only_ids          => NULL,              -- process the shard’s next candidates globally
        p_batch_size        => %(batch_size)s,    -- number of rows for this batch
        p_use_md5           => %(use_md5)s,       -- MD5 vs SHA-256
        p_collapse_ws       => %(collapse_ws)s,   -- normalize XML whitespace
        p_shard_index       => %(shard_index)s,   -- 0..SHARDS-1
        p_shard_count       => %(shard_count)s,   -- total number of shards
        p_run_started_at    => %(run_started_at)s -- the same timestamp for all batches in this run
      )
    )
    SELECT COALESCE(COUNT(*), 0)              AS processed,
           COALESCE(SUM((changed)::int), 0)   AS changed
    FROM run;
    """

    # Each shard opens its own connection. Sharing a single connection across threads is unsafe.
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        batch_no = 0
        while True:
            batch_no += 1
            with conn:  # Start a transaction for this batch
                with conn.cursor() as cur:
                    # Per-batch/session “speed knobs”. LOCAL scopes them to this transaction only.
                    # - synchronous_commit=off: reduce fsync waits for faster WAL commits.
                    # - statement_timeout: protect you from pathological batches getting stuck.
                    # - lock_timeout: fail fast if we can’t acquire a lock quickly.
                    # - idle_in_transaction_session_timeout: prevent a dead client from holding tx.
                    cur.execute("SET LOCAL synchronous_commit = OFF;")
                    cur.execute("SET LOCAL statement_timeout = %s;", ('15min',))
                    cur.execute("SET LOCAL lock_timeout = %s;", ('5s',))
                    cur.execute("SET LOCAL idle_in_transaction_session_timeout = %s;", ('5min',))

                    # Optional: per-session memory. Use judiciously; this is per *sort/hash*.
                    if SET_WORK_MEM:
                        cur.execute("SET LOCAL work_mem = %s;", (SET_WORK_MEM,))

                    # Optional: extra intra-query parallelism.
                    # Sharding is already parallel; keeping this small or None is usually fine.
                    if SET_PARALLEL_PER_GATHER is not None:
                        cur.execute(
                            "SET LOCAL max_parallel_workers_per_gather = %s;",
                            (SET_PARALLEL_PER_GATHER,)
                        )

                    # Do one batch for this shard and read aggregated counts.
                    cur.execute(SQL_BATCH, {
                        "batch_size":       BATCH_SIZE,
                        "use_md5":          USE_MD5,
                        "collapse_ws":      COLLAPSE_WHITESPACE,
                        "shard_index":      shard_index,
                        "shard_count":      SHARDS,
                        "run_started_at":   run_started_at
                    })
                    row = cur.fetchone()
                    if row is None:
                        # Defensive: SQL_BATCH always returns exactly one row,
                        # but guard against None to avoid exploding.
                        processed = changed = 0
                    else:
                        processed, changed = row

            # Accumulate totals per shard for final summary
            processed_total += processed
            changed_total   += changed

            logger.info(
                f"[shard {shard_index}] batch {batch_no}: "
                f"processed={processed:,} changed={changed:,} "
                f"(cum processed={processed_total:,})"
            )

            # Termination condition: when the batch returns 0 processed rows,
            # there’s nothing left to do for this shard.
            if processed == 0:
                break

            # Optional throttle to smooth IO/WAL pressure between batches
            if SLEEP_BETWEEN_BATCHES:
                time.sleep(SLEEP_BETWEEN_BATCHES)

    finally:
        # Always close the shard connection
        conn.close()

    return processed_total, changed_total, time.time() - t0


def main():
    logger.info("=== Refresh run started ===")
    t0 = time.time()

    # Critical: we capture a *single* DB-side timestamp and pass it to every shard/batch.
    # That way the server-side function can:
    #   - only consider rows with last_checked_at < run_started_at
    #   - stamp last_checked_at for rows it processes
    #
    # This prevents a shard from immediately reprocessing rows it (or another shard)
    # just touched a moment ago.

    run_started_at = get_db_now()
    logger.info(f"Run timestamp (p_run_started_at) = {run_started_at.isoformat()}")

    totals_processed = 0
    totals_changed   = 0

    # Create a thread pool: one future per shard. Each shard processes its modulus slice
    # (pkid % SHARDS == shard_index) in independent batches until it returns processed=0.
    with ThreadPoolExecutor(max_workers=SHARDS) as ex:
        futures = {ex.submit(run_shard, i, run_started_at): i for i in range(SHARDS)}
        for fut in as_completed(futures):
            shard = futures[fut]
            try:
                processed, changed, secs = fut.result()
                totals_processed += processed
                totals_changed   += changed
                logger.info(
                    f"[shard {shard}] DONE: processed={processed:,}, "
                    f"changed={changed:,} in {secs:,.1f}s"
                )
            except Exception as e:
                # If a shard fails, we log it and continue. Other shards keep working.
                logger.exception(f"[shard {shard}] FAILED: {e}")

    dt = time.time() - t0
    logger.info(
        f"=== All shards complete: processed={totals_processed:,}, "
        f"changed={totals_changed:,}, elapsed={dt:,.1f}s ==="
    )

    # Optional post-run maintenance: this keeps planner stats current and
    # tidies up dead tuples in the digest table. Cheap and helpful after big upserts.
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


def get_db_now():
    """
    Fetch the current timestamp from the database server (not the app host) so all shards
    use the exact same reference time in their logic. This avoids drift if app and DB clocks
    differ slightly and keeps the “don’t reprocess freshly-checked rows” guarantee solid.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT now()")
            return cur.fetchone()[0]
    finally:
        conn.close()


if __name__ == "__main__":
    main()
