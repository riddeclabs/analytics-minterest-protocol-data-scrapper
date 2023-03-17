import logging
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from config import API_URL
from utils import Tables, sql, types


def __get_all_user_addresses() -> dict:
    df = sql.read("SELECT DISTINCT user_address FROM indexer.user_market_state")

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
    dtype = {
        "date": types.DATETIME,
        "user_address": types.NVARCHAR(64),
        "data": types.JSON,
    }

    return sql.save(df, Tables.RAW_USERS, dtype)


def run_raw_users_pipeline():
    logging.info("Running raw users pipeline")
    addresses = __get_all_user_addresses()
    logging.info(f"Found {len(addresses)} user addresses in DB")

    users = [
        __get_raw_user_data(address)
        for address in tqdm(addresses, desc="Fetching users data")
    ]

    __save_raw_users(addresses, users)

    logging.info(f"Successfully saved {len(users)} raw users into DB")
