import logging
import os
import psycopg2

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     "HOST.NAME",
    "port":     5432,
    "dbname":   "DATABSE",
    "user":     "USER",
    "password": "SECRET"
}

# Batch and vacuum settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))
CHUNKS_PER_VAC = int(os.getenv("CHUNKS_PER_VAC", 10))

def main():
    # Connect to the target database
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        last_id = 0
        chunk = 1

        while True:
            logger.info(f"Chunk #{chunk}: processing up to {BATCH_SIZE} rows from ID > {last_id}...")
            with conn:
                with conn.cursor() as cur:
                    # Tune per‐chunk memory
                    cur.execute("SET work_mem = '64MB';")
                    cur.execute("SET maintenance_work_mem = '256MB';")

                    # Call the batch loader function
                    cur.execute(
                        "SELECT factory.get_filemetadata_batch(%s, %s);",
                        (last_id, BATCH_SIZE),
                    )
                    new_last_id = cur.fetchone()[0]

                    processed = new_last_id - last_id
                    logger.info(f"Rows processed in this chunk: {processed}")

                    if processed <= 0:
                        logger.info("No more rows to process. Exiting loop.")
                        break

                    last_id = new_last_id

            # Periodic VACUUM ANALYZE on the target table
            if chunk % CHUNKS_PER_VAC == 0:
                logger.info("Running VACUUM ANALYZE on factory.filemetadata")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("VACUUM ANALYZE factory.filemetadata;")
                conn.autocommit = False

            chunk += 1

    finally:
        conn.close()
        logger.info("Connection closed.")

if __name__ == "__main__":
    main()
