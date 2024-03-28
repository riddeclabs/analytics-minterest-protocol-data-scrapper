import logging

import pandas as pd

from utils import Tables, sql


def __map_oracle_prices(df: pd.DataFrame) -> pd.DataFrame:
    results = []

    for record in df.to_dict("records"):
        if "error" in record["data"]:
            logging.info(f"Found failed oracle price query: {record}")
            continue

        result = {"date": record["date"]} | __map_price_data(record["data"])
        results.append(result)

    df = pd.DataFrame(results)
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.floor("H")

    return df.drop_duplicates(["date"], keep="first")


def __map_price_data(prices: dict) -> dict:
    result = {"mnt": float(prices["mntOraclePriceUSD"])}

    for market in prices["markets"]:
        result[market["symbol"].lower()] = market["oraclePriceUSD"]

    return result


def run_curated_oracle_prices_pipeline(max_date: pd.Timestamp = None):
    logging.info("Running curated oracle prices pipeline")

    raw = sql.get_unprocessed_raw_data(
        table_name=Tables.ORACLE_PRICES,
        raw_table_name=Tables.RAW_ORACLE_PRICES,
        max_date=max_date,
    )

    if raw.empty:
        logging.info("No raw oracle prices found to process")
        return

    dates = raw["date"].unique()
    logging.info(
        f"Found {len(dates)} raw oracle price records to process from {dates[0]}"
    )

    oracle_prices = __map_oracle_prices(raw)
    sql.save(oracle_prices, Tables.ORACLE_PRICES)

    logging.info(
        f"Successfully saved {len(oracle_prices)} oracle price records into curated DB"
    )
