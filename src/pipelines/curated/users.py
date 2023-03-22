import logging

import pandas as pd

from utils import Tables, sql, types

from .user_markets import __map_user_market_data__


def __map_users(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for record in df.to_dict("records"):
        result = {
            "date": record["date"],
            "user_address": record["user_address"],
        } | __map_user_data(record["data"])
        results.append(result)

    df = pd.DataFrame(results)
    df["date"] = df["date"].dt.strftime("%y-%m-%d %H:00:00")

    return df.drop_duplicates(["date", "user_address"], keep="first")


def __map_user_data(user: dict) -> dict:
    mnt_price = float(user.get("mntPriceUSD", "2.0"))
    total_earned_mnt = (
        float(user["withdraw"]["mntTotalBalance"]) / 10e17
        if "withdraw" in user
        else None
    )

    result = {
        "total_net_apy": round(float(user["totalNetApy"]), 4),
        "net_interest": round(float(user["netInterest"]), 4),
        "emissions": round(float(user["emissions"]), 4),
        "gov_reward": round(float(user["govReward"]), 4),
        "total_supply_usd": round(float(user["userTotalSupplyUSD"]), 4),
        "total_borrow_usd": round(float(user["userTotalBorrowUSD"]), 4),
        "total_collateral_usd": round(float(user["userTotalCollateralUSD"]), 4),
        "mnt_withdrawable": round(
            float(user["userMntWithdrawableUSD"]) / 10e17 / mnt_price, 4
        ),
        "mnt_withdrawable_usd": round(float(user["userMntWithdrawableUSD"]) / 10e17, 4),
        "buy_back_rewards_usd": round(float(user["userBuyBackRewardsUSD"]) / 10e17, 4),
        "participating": user["participating"],
        "is_whitelisted": user["isWhitelisted"],
        "governance_reward_apy": round(float(user["mntAPY"]), 4),
        "collateral_ratio": round(float(user["collateralRatio"]), 4),
        "max_collateral_ratio": round(float(user["maxCollateralRatio"]), 4),
        "loyalty_group": user["userLoyaltyGroup"],
        "loyalty_factor": round(float(user["userLoyaltyFactor"]), 4),
        "total_vesting_locked": round(float(user["vesting"]["totalAmount"]), 4),
        "total_earned_mnt": round(total_earned_mnt, 4) if total_earned_mnt else None,
        "total_earned_mnt_usd": round(total_earned_mnt * mnt_price, 4)
        if total_earned_mnt
        else None,
    }

    # Make the table wide by appending the market name to every key of the market value within user stats
    # e.g, mweth_supply_usd, mwbtc_supply_usd, etc.
    for market in user["userMarkets"]:
        market_result = __map_user_market_data__(market)

        symbol = market_result["symbol"]
        del market_result["symbol"]

        for key, value in market_result.items():
            result[f"{symbol}_{key}"] = value

    return result


def run_curated_users_pipeline():
    logging.info("Running curated users pipeline")

    raw = sql.get_unprocessed_raw_data(
        table_name=Tables.USERS_HISTORY,
        raw_table_name=Tables.RAW_USERS,
    )

    if raw.empty:
        logging.info("No raw users found to process")
        return

    dates = raw["date"].unique()
    logging.info(f"Found {len(dates)} raw users records to process from {dates[0]}")

    users = __map_users(raw)
    latest_users = users.drop_duplicates(["user_address"], keep="last")
    dtype = {
        "date": types.DATETIME,
        "user_address": types.NVARCHAR(64),
    }

    sql.save(users, Tables.USERS_HISTORY, dtype=dtype)
    sql.save(latest_users, Tables.USERS_LATEST, dtype=dtype, replace=True)

    logging.info(f"Successfully saved {len(users)} user records into curated DB")
