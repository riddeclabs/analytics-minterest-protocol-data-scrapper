import logging
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from config import API_URL, INDEXER_DB_NAME
from utils import Tables, sql, types

from .oracle_prices import __get_oracle_prices__

session = requests.session()


def __get_all_user_addresses() -> dict:
    df = sql.read(f"SELECT DISTINCT user_address FROM {INDEXER_DB_NAME}.user_market_state")

    return df["user_address"].to_list()


def __get_raw_user_data(address: str, mnt_price: str) -> dict:
    user_data = session.get(f"{API_URL}/user/data/{address}", timeout=10).json()
    withdraw_data = session.get(
        f"{API_URL}/user/mnt/withdraw/{address}", timeout=10
    ).json()

    user_data["user_address"] = address
    user_data["withdraw"] = withdraw_data
    user_data["mntPriceUSD"] = mnt_price

    return user_data


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

    mnt_price = __get_oracle_prices__()["mntOraclePriceUSD"]

    users = [
        __get_raw_user_data(address, mnt_price)
        for address in tqdm(addresses, desc="Fetching users data")
    ]

    __save_raw_users(addresses, users)

    logging.info(f"Successfully saved {len(users)} raw users into DB")
