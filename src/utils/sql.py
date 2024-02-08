import json

import pandas as pd
from sqlalchemy import create_engine, inspect

from config import SQL_CONNECTION_STRING
import logging

__ENGINE = create_engine(SQL_CONNECTION_STRING)


def read(sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, con=__ENGINE)


def save(
    df: pd.DataFrame,
    table_name: str,
    dtype: dict[str, any],
    replace: bool = False,
) -> int:
    return df.to_sql(
        table_name,
        con=__ENGINE,
        if_exists="replace" if replace else "append",
        index=False,
        dtype=dtype,
    )


def get_latest_date(table_name: str) -> pd.Timestamp:
    if inspect(__ENGINE).has_table(table_name):
        max_date = pd.read_sql_query(
            f"SELECT MAX(date) date from {table_name}", con=__ENGINE
        )["date"]
        return max_date[0] if len(max_date) else pd.Timestamp.min

    return pd.Timestamp.min


def get_unprocessed_raw_data(table_name: str, raw_table_name: str, max_date: pd.Timestamp = None) -> pd.DataFrame:
    latest_date = get_latest_date(table_name) + pd.Timedelta(1, "hour")
    logging.info(f"Getting unprocessed data from {raw_table_name} since {latest_date} to {max_date or 'now'}")

    sql = f"SELECT * FROM {raw_table_name} WHERE date > '{latest_date}'"

    if max_date:
        sql += f" and date < '{max_date}'"

    df = pd.read_sql(sql, con=__ENGINE)
    df["data"] = df["data"].map(json.loads)

    return df
