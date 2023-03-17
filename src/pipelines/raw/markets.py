import logging
from datetime import datetime

import pandas as pd
import requests

from config import API_URL
from utils import Tables, sql, types


def __get_raw_markets() -> dict:
    result = requests.get(f"{API_URL}/markets", timeout=10)

    return result.json()


def __save_raw_markets(markets: dict):
    df = pd.DataFrame({"date": [datetime.utcnow()], "data": [markets]})
    dtype = {
        "date": types.DATETIME,
        "data": types.JSON,
    }

    sql.save(df, Tables.RAW_MARKETS, dtype)


def run_raw_markets_pipeline():
    logging.info("Running raw markets pipelines")

    markets = __get_raw_markets()
    __save_raw_markets(markets)

    logging.info(f"Successfully saved {len(markets)} markets into DB")
