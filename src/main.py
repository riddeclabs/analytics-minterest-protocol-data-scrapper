import logging

import coloredlogs
import argparse

import pipelines
from config import API_URL, INDEXER_DB_NAME, SQL_CONNECTION_STRING

coloredlogs.install()
logging.info(f"Going to read data from '{API_URL}' and indexer db '{INDEXER_DB_NAME}' and write it into '{SQL_CONNECTION_STRING.split('@')[-1]}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--currated-only", action="store_true", default=False, help="Run currated pipelines only")
    parser.add_argument("--max-date", type=str, default=None, help="Run pipelines only for data before this date")
    args = parser.parse_args()

    if not args.currated_only:
        pipelines.run_raw_markets_pipeline()
        pipelines.run_raw_oracle_prices_pipeline()
        pipelines.run_raw_users_pipeline()

    pipelines.run_curated_markets_pipeline(max_date=args.max_date)
    pipelines.run_curated_oracle_prices_pipeline(max_date=args.max_date)
    pipelines.run_curated_users_pipeline(max_date=args.max_date)
    pipelines.run_curated_user_markets_pipeline(max_date=args.max_date)
