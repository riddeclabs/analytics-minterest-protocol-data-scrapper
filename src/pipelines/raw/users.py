import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm

from config import (
    MAX_TREADS_FOR_USERS_FETCHING,
    RETRIES_COUNT_FOR_USERS_FETCHING,
)
from utils import DataFetcher, Tables, s3, sql
from data import indexer_data

from .oracle_prices import __get_oracle_prices__


def __get_raw_user_data(
    data_fetcher: DataFetcher, address: str, oracle_prices: dict
) -> dict:
    mintyMarket = next(
        filter(lambda x: x["symbol"] == "mWMNT", oracle_prices["markets"]), None
    )

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


def __save_raw_users(addresses: list[str], users: list[dict]) -> None:
    df = pd.DataFrame(
        {
            "date": pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0),
            "user_address": addresses,
            "data": users,
        }
    )

    s3.save(df, Tables.RAW_USERS)
    sql.save(df, Tables.RAW_USERS_LATEST, replace=True)


def __fetch_user_data() -> tuple[list[str], list[dict]]:
    addresses = indexer_data.get_all_user_addresses()
    logging.info(f"Found {len(addresses)} user addresses in DB")

    data_fetcher = DataFetcher(retries=RETRIES_COUNT_FOR_USERS_FETCHING)
    oracle_prices = __get_oracle_prices__(data_fetcher)

    with ThreadPoolExecutor(max_workers=MAX_TREADS_FOR_USERS_FETCHING) as executor:
        users = list(
            tqdm(
                executor.map(
                    lambda address: __get_raw_user_data(
                        data_fetcher, address, oracle_prices
                    ),
                    addresses,
                ),
                total=len(addresses),
                desc="Fetching users data",
            )
        )

    return addresses, users


def run_raw_users_pipeline(force: bool = False):
    logging.info(f"Running raw users pipeline with force flag = {force}")

    now = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0).tz_convert(None)
    latest_date = s3.get_latest_date(Tables.RAW_USERS)

    logging.info(f"Latest date for users data in DB: {latest_date}")

    if latest_date and now <= latest_date and not force:
        logging.info("No new user data found to fetch")
        return

    addresses, users = __fetch_user_data()
    __save_raw_users(addresses, users)

    logging.info(f"Successfully saved {len(users)} raw users into DB")
