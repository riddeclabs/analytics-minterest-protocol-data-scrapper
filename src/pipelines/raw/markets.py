import logging
from datetime import datetime

import pandas as pd

from utils import DataFetcher, Tables, sql, types


def __save_raw_markets(markets: dict):
    df = pd.DataFrame({"date": [datetime.utcnow()], "data": [markets]})
    dtype = {
        "date": types.DATETIME,
        "data": types.JSON,
    }

    sql.save(df, Tables.RAW_MARKETS, dtype)


def run_raw_markets_pipeline():
    logging.info("Running raw markets pipelines")

    data_fetcher = DataFetcher()
    markets = data_fetcher.fetch("markets")

    __save_raw_markets(markets)

    logging.info(f"Successfully saved {len(markets)} markets into DB")
