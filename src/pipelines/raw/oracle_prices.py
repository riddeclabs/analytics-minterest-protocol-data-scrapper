import logging
from datetime import datetime

import pandas as pd

from utils import DataFetcher, Tables, sql


def __get_oracle_prices__(data_fetcher: DataFetcher) -> dict:
    oracle_prices = data_fetcher.fetch("utils/oracle-price")

    return oracle_prices


def __save_oracle_prices(markets: dict):
    df = pd.DataFrame(
        {
            "date": [datetime.utcnow().replace(minute=0, second=0, microsecond=0)],
            "data": [markets],
        }
    )

    sql.save(df, Tables.RAW_ORACLE_PRICES, replace_by_date=True)


def run_raw_oracle_prices_pipeline(force: bool = False):
    logging.info(f"Running raw oracle prices pipelines with force flag = {force}")

    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    latest_date = sql.get_latest_date(Tables.RAW_ORACLE_PRICES)

    logging.info(f"Latest date for oracle prices in DB: {latest_date}")

    if latest_date and now <= latest_date and not force:
        logging.info("No new oracle prices found to fetch")
        return

    data_fetcher = DataFetcher()
    oracle_prices = __get_oracle_prices__(data_fetcher)

    __save_oracle_prices(oracle_prices)

    logging.info(f"Successfully saved {len(oracle_prices)} oracle prices into DB")
