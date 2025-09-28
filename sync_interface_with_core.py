import logging
import sys

import psycopg2

from database_config import DB_CONFIG

# log file
LOG_FILENAME = "sync_interface_with_core.log"

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

    try:
        with conn:
            with conn.cursor() as cur:
                logger.info("Started core.sync_interface_with_core()")
                cur.execute("SET work_mem = '4GB'")
                cur.execute("SET maintenance_work_mem = '2GB'")
                cur.execute("SELECT core.sync_interface_with_core()")

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
    finally:
        conn.close()
        logger.info("=== Run finished ===")


if __name__ == "__main__":
    main()
