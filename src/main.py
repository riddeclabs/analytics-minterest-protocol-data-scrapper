import logging

import coloredlogs
import argparse
import time
import pandas as pd

import pipelines
from utils import sql, Tables
from config import (
    API_URL,
    INDEXER_DB_SQL_CONNECTION_STRING,
    ANALYTICS_DB_SQL_CONNECTION_STRING,
    S3_BUCKET,
    IS_MANTLE_NETWORK,
    IS_ETHEREUM_NETWORK,
)

coloredlogs.install()
logging.info(
    f"Going to read data from '{API_URL}' and indexer db '{INDEXER_DB_SQL_CONNECTION_STRING.split('@')[-1]}' "
    + f"and write it into '{ANALYTICS_DB_SQL_CONNECTION_STRING.split('@')[-1]}' PgSQL DB and {S3_BUCKET} s3 bucket."
)


def report_pipeline_status(start_date: float, status: str):
    duration_mins = round((time.time() - start_date) / 60, 1)
    logging.info(
        f"Finished running all pipelines in {duration_mins} minutes with status '{status}'"
    )

    df = pd.DataFrame(
        {
            "start_date": [pd.Timestamp(start_date * 1e9)],
            "duration_mins": [duration_mins],
            "status": [status],
        }
    )

    sql.save(df, Tables.PIPELINES_STATUS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--currated-only",
        action="store_true",
        default=False,
        help="Run currated pipelines only",
    )
    parser.add_argument(
        "--max-date",
        type=str,
        default=None,
        help="Run pipelines only for data before this date",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force run raw pipelines",
    )
    args = parser.parse_args()

    start = time.time()

    try:
        if not args.currated_only:
            pipelines.run_raw_markets_pipeline(force=args.force)
            pipelines.run_raw_oracle_prices_pipeline(force=args.force)
            pipelines.run_raw_users_pipeline(force=args.force)

        pipelines.run_curated_oracle_prices_pipeline(max_date=args.max_date)
        pipelines.run_curated_markets_pipeline(max_date=args.max_date)
        pipelines.run_curated_users_pipeline(max_date=args.max_date)
        pipelines.run_curated_user_markets_pipeline(max_date=args.max_date)
        pipelines.run_curated_user_transactions_pipeline()
        pipelines.run_curated_nft_tiers_pipeline()
        pipelines.run_curated_nft_transactions_pipeline()

        if IS_MANTLE_NETWORK:
            pipelines.run_curated_liquidations_pipeline()
        elif IS_ETHEREUM_NETWORK:
            pipelines.run_curated_vesting_refund_poll_pipeline()
            pipelines.run_curated_cs_issues_tracker_google_sheets_export_pipeline()
            pipelines.run_curated_mossbets_user_data_google_sheets_export_pipeline()

        report_pipeline_status(start, "success")
    except KeyboardInterrupt:
        logging.warning("Keyboard interrupt received. Stopping the pipelines.")
        report_pipeline_status(start, "interrupted")
    except Exception as e:
        report_pipeline_status(start, "failed")
        raise e
