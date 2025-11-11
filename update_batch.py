import logging
import sys
import time

import psycopg2

from database_config import DB_CONFIG

BATCH_SIZE = 1000
SLEEP_BETWEEN_CHUNKS = 0
LOG_FILENAME = "update_batch.log"

logger = logging.getLogger("batch_updater")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)

fh = logging.FileHandler(LOG_FILENAME)
fh.setFormatter(fmt)
logger.addHandler(fh)

UPDATE_SQL = """
WITH chunk AS (
  SELECT titleinformation_pkid
  FROM interface.titleinformation_interface
  WHERE titleinformation_pkid > %s
  ORDER BY titleinformation_pkid
  LIMIT %s
  FOR UPDATE SKIP LOCKED
)
UPDATE interface.titleinformation_interface i
SET title = title 
FROM chunk
WHERE i.titleinformation_pkid = chunk.titleinformation_pkid
RETURNING i.titleinformation_pkid;
"""


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        last_id = 0
        chunk = 1
        while True:
            logger.info(f"Chunk #{chunk}: updating up to {BATCH_SIZE} rows where id > {last_id}...")
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SET work_mem = '4GB';")
                    cur.execute("SET maintenance_work_mem = '1GB';")

                    cur.execute(UPDATE_SQL, (last_id, BATCH_SIZE))
                    rows = cur.fetchall()  # list of (ids)
                    if not rows:
                        logger.info("No more rows to update. Exiting loop.")
                        break

                    ids = [r[0] for r in rows]
                    processed = len(ids)
                    new_last_id = max(ids)

                    logger.info(f"Rows updated in this chunk: {processed}; highest id: {new_last_id}")

                    last_id = new_last_id

            chunk += 1
            if SLEEP_BETWEEN_CHUNKS > 0:
                time.sleep(SLEEP_BETWEEN_CHUNKS)

        logger.info("Done.")
    finally:
        conn.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    main()
