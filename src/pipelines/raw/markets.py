import logging
from datetime import datetime

import pandas as pd

from utils import DataFetcher, Tables, sql


def __save_raw_markets(markets: dict):
    df = pd.DataFrame(
        {
            "date": [datetime.utcnow().replace(minute=0, second=0, microsecond=0)],
            "data": [markets],
        }
    )

    sql.save(df, Tables.RAW_MARKETS, replace_by_date=True)


def run_raw_markets_pipeline(force: bool = False):
    logging.info(f"Running raw markets pipelines with force flag = {force}")

    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    latest_date = sql.get_latest_date(Tables.RAW_MARKETS)

    logging.info(f"Latest date for markets data in DB: {latest_date}")

    if latest_date and now <= latest_date and not force:
        logging.info("No new market records found to fetch")
        return

    data_fetcher = DataFetcher()
    markets = data_fetcher.fetch("markets")

    __save_raw_markets(markets)

    logging.info(f"Successfully saved {len(markets)} markets into DB")
