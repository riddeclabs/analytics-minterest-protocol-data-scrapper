import logging

import pandas as pd

from utils import Tables, sql, Types


def __map_markets(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for record in df.to_dict("records"):
        if "error" in record["data"]:
            logging.info(f"Found failed market query: {record}")
            continue

        for market in record["data"]["markets"]:
            result = {"date": record["date"]} | __map_market_data(market)
            results.append(result)

    df = pd.DataFrame(results)

    return df.drop_duplicates(["date", "symbol"], keep="first")


def __map_market_data(market: dict) -> dict:
    values = {}

    for key in market["economic"]:
        values[key] = round(float(market["economic"][key]), 4)

    result = {
        "symbol": market["meta"]["symbol"],
        "number_of_suppliers": market["statistics"]["numberOfSuppliers"],
        "number_of_borrowers": market["statistics"]["numberOfBorrowers"],
        "supply": values["marketSupplyUnderlying"],
        "supply_usd": values["marketSupplyUSD"],
        "borrow": values["marketBorrowUnderlying"],
        "borrow_usd": values["marketBorrowUSD"],
        "liquidity": values["marketLiquidityUnderlying"],
        "locked": values["marketValueLocked"],
        "locked_usd": values["marketValueLockedUSD"],
        "reserves": values["marketReservesUnderlying"],
        "apy": values["apy"],
        "apr": values["apr"],
        "mnt_supply_apy": values["marketMntSupplyAPY"],
        "mnt_borrow_apy": values["marketMntBorrowAPY"],
        "utilisation_rate": values["utilisationRate"],
        "reserve_rate": values["reserveRate"],
        "utilisation_factor": values["utilisationFactor"],
    }

    # Do it for mantle market only
    if "marketMantleSupplyAPY" in values:
        result["mantle_supply_apy"] = values["marketMantleSupplyAPY"]
        result["mantle_borrow_apy"] = values["marketMantleBorrowAPY"]

    return result


def run_curated_markets_pipeline(max_date: pd.Timestamp = None):
    logging.info("Running curated markets pipeline")

    raw = sql.get_unprocessed_raw_data(
        table_name=Tables.MARKETS_HISTORY,
        raw_table_name=Tables.RAW_MARKETS,
        max_date=max_date,
    )

    if raw.empty:
        logging.info("No raw markets found to process")
        return

    dates = raw["date"].unique()
    logging.info(f"Found {len(dates)} raw markets records to process from {dates[0]}")

    markets = __map_markets(raw)
    latest_markets = markets.drop_duplicates(["symbol"], keep="last")
    dtype = {
        "date": Types.DateTime,
        "symbol": Types.String(5),
    }

    sql.save(markets, Tables.MARKETS_HISTORY, dtype=dtype)
    sql.save(latest_markets, Tables.MARKETS_LATEST, dtype=dtype, replace=True)

    logging.info(f"Successfully saved {len(markets)} market records into curated DB")
