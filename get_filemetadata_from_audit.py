import logging
import sys
import time

import psycopg2
from psycopg2 import errors

from database_config import DB_CONFIG

BATCH_SIZE = 100  # rows per chunk
CHUNKS_PER_VAC = 100  # vacuum every N chunks
SLEEP_BETWEEN_CHUNKS = 0  # seconds (set to positive int if you want to throttle)
LOOKBACK_INTERVAL = "7 days"  # the lookback interval param for the function

# log file
LOG_FILENAME = "get_filemetadata_from_audit.log"

# log handler
logger = logging.getLogger("batch_processor")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# console handler
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)

# file handler
fh = logging.FileHandler(LOG_FILENAME)
fh.setFormatter(fmt)
logger.addHandler(fh)


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    # guards: stop after N chunks or T minutes even if DB never says "done"
    MAX_CHUNKS = 10_000
    MAX_RUNTIME = 60 * 10

    start_ts = time.time()
    last_mtime = None  # keyset cursor (None means first page)
    last_id = None
    chunk = 1

    try:
        while True:
            # time guard
            if time.time() - start_ts > MAX_RUNTIME:
                logger.info("Max runtime reached — exiting loop gracefully.")
                break
            if chunk > MAX_CHUNKS:
                logger.info("Max chunks reached — exiting loop gracefully.")
                break

            logger.info(f"Chunk #{chunk}: up to {BATCH_SIZE} rows since {LOOKBACK_INTERVAL}, "
                        f"starting after ({last_mtime}, {last_id})")

            try:
                with conn:
                    with conn.cursor() as cur:
                        # keep per-chunk limits so a single slow batch won’t hang forever
                        cur.execute("SET statement_timeout = '10min';")
                        cur.execute("SET work_mem = '4GB';")
                        cur.execute("SET maintenance_work_mem = '2GB';")
                        cur.execute("SET application_name = 'fm_audit_loader';")

                        cur.execute(
                            """
                            SELECT next_mtime, next_id, processed
                            FROM factory.get_filemetadata_batch_to_audit(%s, %s, %s, %s::interval);
                            """,
                            (last_mtime, last_id, BATCH_SIZE, LOOKBACK_INTERVAL),
                        )
                        row = cur.fetchone()
                        if not row:
                            logger.info("Function returned no row; exiting loop.")
                            break
                        next_mtime, next_id, processed = row
                        logger.info(f"Rows processed in this chunk: {processed}")

                        # Normal end-of-window signal from the function
                        if processed is None or processed <= 0:
                            logger.info("No more rows in window (processed<=0). Exiting loop.")
                            break

                        # No-progress guard: don’t spin forever if the cursor didn’t move
                        if next_mtime == last_mtime and next_id == last_id:
                            logger.info("Cursor did not advance — exiting loop to avoid spin.")
                            break

                        # Advance the keyset cursor
                        last_mtime, last_id = next_mtime, next_id

                # Periodic VACUUM ANALYZE on the target table
                if chunk % CHUNKS_PER_VAC == 0:
                    logger.info("Running VACUUM ANALYZE on factory.filemetadata_conversion_audit_prd")
                    conn.autocommit = True
                    with conn.cursor() as cur:
                        cur.execute("VACUUM ANALYZE factory.filemetadata_conversion_audit_prd;")
                    conn.autocommit = False

                # Optional throttle
                if SLEEP_BETWEEN_CHUNKS > 0:
                    time.sleep(SLEEP_BETWEEN_CHUNKS)

                chunk += 1

            except errors.QueryCanceled as e:  # statement_timeout hit
                logger.warning("Statement timeout hit — exiting loop gracefully: %s", e)
                break

            except Exception as e:
                logger.exception("Unexpected error in chunk — exiting loop: %s", e)
                break

    finally:
        conn.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    main()
