import logging

import pandas as pd

from utils import Tables, sql, types


def __map_user_markets(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for record in df.to_dict("records"):
        for market in record["data"]["userMarkets"]:
            result = {
                "date": record["date"],
                "user_address": record["user_address"],
            } | __map_user_market_data__(market)
            results.append(result)

    df = pd.DataFrame(results)
    df["date"] = df["date"].dt.strftime("%y-%m-%d %H:00:00")

    return df.drop_duplicates(["date", "user_address", "symbol"], keep="first")


def __map_user_market_data__(market: dict) -> dict:
    return {
        "symbol": market["symbol"],
        "supply": round(float(market["userSupplyUnderlying"]), 3),
        "supply_usd": round(float(market["userSupplyUSD"]), 3),
        "borrow": round(float(market["userBorrowUnderlying"]), 3),
        "borrow_usd": round(float(market["userBorrowUSD"]), 3),
        "balance": round(float(market["underlyingBalance"]), 3),
        "net_apy": round(float(market["netApy"]), 2),
        "annual_income_usd": round(float(market["annualIncome"]) / 10e18, 3),
        "mnt_supply_apy": round(float(market["mntSupplyAPY"]), 3),
        "mnt_borrow_apy": round(float(market["mntBorrowAPY"]), 3),
        "collateral_status": market["collateralStatus"],
        "collateral_usd": round(float(market["userMarketCollateralUSD"]) / 10e18, 3),
        "apy": round(float(market["apy"]) / 10e16, 3),
        "apr": round(float(market["apr"]) / 10e16, 3),
    }


def run_curated_user_markets_pipeline():
    logging.info("Running curated users pipeline")

    raw = sql.get_unprocessed_raw_data(
        table_name=Tables.USER_MARKETS_HISTORY,
        raw_table_name=Tables.RAW_USERS,
    )

    if raw.empty:
        logging.info("No raw users found to process")
        return

    dates = raw["date"].unique()
    logging.info(f"Found {len(dates)} raw users records to process from {dates[0]}")

    users = __map_user_markets(raw)
    latest_users = users.drop_duplicates(["user_address", "symbol"], keep="last")
    dtype = {
        "date": types.DATETIME,
        "user_address": types.NVARCHAR(64),
        "symbol": types.NVARCHAR(5),
    }

    sql.save(users, Tables.USER_MARKETS_HISTORY, dtype=dtype)
    sql.save(latest_users, Tables.USER_MARKETS_LATEST, dtype=dtype, replace=True)

    logging.info(f"Successfully saved {len(users)} user market records into curated DB")
