import logging
from datetime import datetime

import pandas as pd
import requests

from config import API_URL
from utils import Tables, sql, types


def __get_oracle_prices__() -> dict:
    result = requests.get(f"{API_URL}/utils/oracle-price", timeout=10)

    return result.json()


def __save_oracle_prices(markets: dict):
    df = pd.DataFrame({"date": [datetime.utcnow()], "data": [markets]})
    dtype = {
        "date": types.DATETIME,
        "data": types.JSON,
    }

    sql.save(df, Tables.RAW_ORACLE_PRICES, dtype)


def run_raw_oracle_prices_pipeline():
    logging.info("Running raw oracle prices pipelines")

    markets = __get_oracle_prices__()
    __save_oracle_prices(markets)

    logging.info("Successfully saved oracle prices into DB")
