from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, types

from config import API_URL, SQL_CONNECTION_STRING

__ENGINE = create_engine(SQL_CONNECTION_STRING)


def __get_raw_markets() -> dict:
    result = requests.get(f"{API_URL}/markets", timeout=10)

    return result.json()


def __save_raw_markets(markets: dict):
    df = pd.DataFrame({"date": [datetime.utcnow()], "data": [markets]})

    df.to_sql(
        "raw_markets",
        con=__ENGINE,
        if_exists="append",
        index=False,
        dtype={
            "date": types.DATETIME,
            "data": types.JSON,
        },
    )


def run_markets_pipeline():
    markets = __get_raw_markets()
    __save_raw_markets(markets)
