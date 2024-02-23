import logging

import coloredlogs
import argparse

import pipelines
from config import (
    API_URL,
    INDEXER_DB_SQL_CONNECTION_STRING,
    ANALYTICS_DB_SQL_CONNECTION_STRING,
    ATHENA_DB,
)

coloredlogs.install()
logging.info(
    f"Going to read data from '{API_URL}' and indexer db '{INDEXER_DB_SQL_CONNECTION_STRING.split('@')[-1]}' "
    + f"and write it into '{ANALYTICS_DB_SQL_CONNECTION_STRING.split('@')[-1]}' PgSQL DB and {ATHENA_DB} Athena DB."
)

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

    if not args.currated_only:
        pipelines.run_raw_markets_pipeline(force=args.force)
        pipelines.run_raw_oracle_prices_pipeline(force=args.force)
        pipelines.run_raw_users_pipeline(force=args.force)

    pipelines.run_curated_markets_pipeline(max_date=args.max_date)
    pipelines.run_curated_oracle_prices_pipeline(max_date=args.max_date)
    pipelines.run_curated_users_pipeline(max_date=args.max_date)
    pipelines.run_curated_user_markets_pipeline(max_date=args.max_date)
    pipelines.run_curated_user_transactions_pipeline()
    pipelines.run_curated_nft_transactions_pipeline()
