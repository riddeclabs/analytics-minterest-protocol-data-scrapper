import logging

import pandas as pd

from utils import Tables, athena, Types, sql
from itertools import groupby
from tqdm.auto import tqdm


def __map_user_markets(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for record in df.to_dict("records"):
        if "error" in record["data"] or "error" in record["data"]["withdraw"]:
            logging.info(f"Found failed user query: {record}")
            continue

        for market in record["data"]["userMarkets"]:
            result = {
                "date": record["date"],
                "user_address": record["user_address"],
            } | __map_user_market_data__(market)
            results.append(result)

    df = pd.DataFrame(results)
    df.insert(1, "time", pd.to_datetime(df["date"]))
    df["date"] = df["time"].dt.floor("d")

    return df.drop_duplicates(["date", "user_address", "symbol"], keep="first")


def __map_user_market_data__(market: dict) -> dict:
    result = {
        "symbol": market["symbol"],
        "supply": round(float(market["userSupplyUnderlying"]), 3),
        "supply_usd": round(float(market["userSupplyUSD"]), 3),
        "borrow": round(float(market["userBorrowUnderlying"]), 3),
        "borrow_usd": round(float(market["userBorrowUSD"]), 3),
        "balance": round(float(market["underlyingBalance"]), 3),
        "net_apy": round(float(market["netApy"]), 2),
        "annual_income_usd": round(float(market["annualIncome"]) / 10e17, 3),
        "mnt_supply_apy": round(float(market["mntSupplyAPY"]), 3),
        "mnt_borrow_apy": round(float(market["mntBorrowAPY"]), 3),
        "collateral_status": market["collateralStatus"],
        "collateral_usd": round(float(market["userMarketCollateralUSD"]) / 10e17, 3),
        "apy": round(float(market["apy"]) / 10e15, 3),
        "apr": round(float(market["apr"]) / 10e15, 3),
    }

    # Do it for mantle market only
    if "mantleSupplyAPY" in market:
        result["mantle_supply_apy"] = round(float(market["mantleSupplyAPY"]), 3)
        result["mantle_borrow_apy"] = round(float(market["mantleBorrowAPY"]), 3)

    return result


def run_curated_user_markets_pipeline(max_date: pd.Timestamp = None):
    logging.info(f"Running curated user markets pipeline with max_date = {max_date}")

    latest_date = sql.get_latest_date(Tables.USER_MARKETS_HISTORY, date_column="time")
    all_dates = athena.get_all_dates(Tables.RAW_USERS, min_date=latest_date)
    dates = [
        max(group) for _, group in groupby(sorted(all_dates), key=lambda x: x.date())
    ]

    if not dates:
        logging.info("No raw user markets found to process")
        return

    logging.info(
        f"Found {len(dates)} user market records to process from {dates[0]} to {dates[-1]}"
    )

    for date in tqdm(dates):
        raw = athena.read_partition(Tables.RAW_USERS, date)
        users = __map_user_markets(raw)
        dtype = {
            "date": Types.Date,
            "time": Types.DateTime,
        }

        sql.save(users, Tables.USER_MARKETS_HISTORY, dtype=dtype, replace_by_date=True)

    sql.save(users, Tables.USER_MARKETS_LATEST, dtype=dtype, replace=True)

    logging.info(f"Successfully saved {len(dates)} user market records into curated DB")
