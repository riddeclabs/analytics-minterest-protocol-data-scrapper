from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, types
from tqdm import tqdm

from config import API_URL, SQL_CONNECTION_STRING

__ENGINE = create_engine(SQL_CONNECTION_STRING)


def __get_all_user_addresses() -> dict:
    df = pd.read_sql_query(
        "SELECT DISTINCT user_address FROM indexer.user_market_state", con=__ENGINE
    )

    return df["user_address"].to_list()


def __get_raw_user_data(address: str) -> dict:
    response = requests.get(f"{API_URL}/user/data/{address}", timeout=10)

    result = response.json()
    result["user_address"] = address

    return result


def __save_raw_users(addresses: list[str], users: list[dict]) -> int:
    df = pd.DataFrame(
        {"date": datetime.utcnow(), "user_address": addresses, "data": users}
    )

    return df.to_sql(
        "raw_users",
        con=__ENGINE,
        if_exists="append",
        index=False,
        dtype={
            "date": types.DATETIME,
            "user_address": types.NVARCHAR(64),
            "data": types.JSON,
        },
    )


def run_users_pipeline() -> int:
    addresses = __get_all_user_addresses()

    users = [
        __get_raw_user_data(address)
        for address in tqdm(addresses, desc="Fetching users data")
    ]

    return __save_raw_users(addresses, users)
