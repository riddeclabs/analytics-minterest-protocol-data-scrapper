import logging
from datetime import datetime

import pandas as pd

from utils import DataFetcher, Tables, sql, types


def __get_oracle_prices__(data_fetcher: DataFetcher) -> dict:
    oracle_prices = data_fetcher.fetch("utils/oracle-price")

    return oracle_prices


def __save_oracle_prices(markets: dict):
    df = pd.DataFrame({"date": [datetime.utcnow()], "data": [markets]})
    dtype = {
        "date": types.DATETIME,
        "data": types.JSON,
    }

    sql.save(df, Tables.RAW_ORACLE_PRICES, dtype)


def run_raw_oracle_prices_pipeline():
    logging.info("Running raw oracle prices pipelines")

    data_fetcher = DataFetcher()
    oracle_prices = __get_oracle_prices__(data_fetcher)

    __save_oracle_prices(oracle_prices)

    logging.info("Successfully saved oracle prices into DB")
