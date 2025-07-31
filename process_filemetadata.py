import psycopg2
import logging
import sys
import time

# ─── CONFIGURATION ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     "HOST.NAME",
    "port":     5432,
    "dbname":   "DATABASE",
    "user":     "USER",
    "password": "SECRET"
}

BATCH_SIZE              = 100                           # rows per chunk
CHUNKS_PER_VAC          = 100                           # vacuum every N chunks
SLEEP_BETWEEN_CHUNKS    = 0                             # seconds (set to positive int if you want to throttle)

LOG_FILENAME            = "process_filemetadata.log"    # the log file

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
    logger.info("=== Run started ===")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Could not connect to DB: {e}")
        sys.exit(1)

    chunk = 1
    try:
        while True:
            # Start a new transaction for this chunk
            with conn:
                with conn.cursor() as cur:
                    logger.info(f"Chunk #{chunk}: processing up to {BATCH_SIZE} rows…")
                    cur.execute("SET work_mem = '64MB'")
                    cur.execute("SET maintenance_work_mem = '256MB'")
                    cur.execute(
                        "SELECT factory.process_filemetadata_chunk(%s)",
                        (BATCH_SIZE,)
                    )
                    rows = cur.fetchone()[0]
            logger.info(f"Rows processed: {rows}")

            if rows == 0:
                logger.info("No more unprocessed rows. Exiting.")
                break

            # Periodic VACUUM ANALYZE
            if chunk % CHUNKS_PER_VAC == 0:
                logger.info("*** Running VACUUM ANALYZE on metadata tables ***")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("VACUUM ANALYZE core.creator")
                    cur.execute("VACUUM ANALYZE core.creator_organisation")
                    cur.execute("VACUUM ANALYZE core.creator_person")
                    cur.execute("VACUUM ANALYZE core.creator_tool")
                    cur.execute("VACUUM ANALYZE core.ipr")
                    cur.execute("VACUUM ANALYZE core.keyword")
                    cur.execute("VACUUM ANALYZE core.language")
                    cur.execute("VACUUM ANALYZE core.languagevariety")
                    cur.execute("VACUUM ANALYZE core.metadata")
                    cur.execute("VACUUM ANALYZE core.person")
                    cur.execute("VACUUM ANALYZE core.source")
                    cur.execute("VACUUM ANALYZE core.source_creator")
                    cur.execute("VACUUM ANALYZE core.sourcecollectionspecificmetadata")
                    cur.execute("VACUUM ANALYZE core.sourcecollectionspecificmetadataelement")
                    cur.execute("VACUUM ANALYZE core.sourcecollectionspecificmetadatagroup")
                    cur.execute("VACUUM ANALYZE core.textcategorisation")
                    cur.execute("VACUUM ANALYZE core.textcategorisation_genresubgenre")
                    cur.execute("VACUUM ANALYZE core.textcategorisation_keyword")
                    cur.execute("VACUUM ANALYZE core.textcategorisation_language")
                    cur.execute("VACUUM ANALYZE core.textcategorisation_languagevariety")
                    cur.execute("VACUUM ANALYZE core.textcategorisation_topic")
                    cur.execute("VACUUM ANALYZE core.textfile")
                    cur.execute("VACUUM ANALYZE core.titleinformation")
                    cur.execute("VACUUM ANALYZE core.topic")
                    cur.execute("VACUUM ANALYZE factory.filemetadata")
                conn.autocommit = False

            chunk += 1
            if SLEEP_BETWEEN_CHUNKS:
                time.sleep(SLEEP_BETWEEN_CHUNKS)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
    finally:
        conn.close()
        logger.info("=== Run finished ===")

if __name__ == "__main__":
    main()
