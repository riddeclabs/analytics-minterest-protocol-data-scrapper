import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from config import (INDEXER_DB_NAME, MAX_TREADS_FOR_USERS_FETCHING,
                    RETRIES_COUNT_FOR_USERS_FETCHING)
from utils import DataFetcher, Tables, sql, types

from .oracle_prices import __get_oracle_prices__


def __get_all_user_addresses() -> dict:
    df = sql.read(f"SELECT DISTINCT user_address FROM {INDEXER_DB_NAME}.user_market_state")

    return df["user_address"].to_list()


def __get_raw_user_data(data_fetcher: DataFetcher, address: str, oracle_prices: dict) -> dict:
    mintyMarket = next(filter(lambda x: x["symbol"] == "mWMNT", oracle_prices["markets"]), None)
    
    user_data = data_fetcher.fetch(f"user/data/{address}")
    withdraw_data = data_fetcher.fetch(f"user/mnt/withdraw/{address}")

    user_data["user_address"] = address
    user_data["withdraw"] = withdraw_data
    user_data["mntPriceUSD"] = oracle_prices["mntOraclePriceUSD"]

    if mintyMarket:
        user_data["incentivePriceUSD"] = mintyMarket["oraclePriceUSD"]

        incentive_withdraw_data = data_fetcher.fetch(f"user/mantle/withdraw/{address}")
        user_data["incentiveWithdraw"] = incentive_withdraw_data

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

    data_fetcher = DataFetcher(retries=RETRIES_COUNT_FOR_USERS_FETCHING)

    oracle_prices = __get_oracle_prices__(data_fetcher)

    with ThreadPoolExecutor(max_workers=MAX_TREADS_FOR_USERS_FETCHING) as executor:
        users = list(
            tqdm(
                executor.map(
                    lambda address: __get_raw_user_data(data_fetcher, address, oracle_prices),
                    addresses,
                ),
                total=len(addresses),
                desc="Fetching users data",
            )
        )

    __save_raw_users(addresses, users)

    logging.info(f"Successfully saved {len(users)} raw users into DB")
