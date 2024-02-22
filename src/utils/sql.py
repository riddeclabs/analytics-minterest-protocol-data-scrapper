import json

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from config import ANALYTICS_DB_SQL_CONNECTION_STRING
from .types import Types


__ENGINE = create_engine(ANALYTICS_DB_SQL_CONNECTION_STRING)


def read(sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, con=__ENGINE)


def __create_dtypes(df: pd.DataFrame, dtype: dict[str, any] = {}) -> dict[str, any]:
    result = {
        column: Types.String(length=64)
        for column, dtype in df.dtypes.items()
        if dtype in [pd.StringDtype]
    }

    if "date" in df.columns:
        result["date"] = Types.DateTime

    if "data" in df.columns:
        result["data"] = Types.JSON

    return result | dtype


def save(
    df: pd.DataFrame,
    table_name: str,
    dtype: dict[str, any] = {},
    replace: bool = False,
    replace_by_date: bool = False,
) -> int:
    with __ENGINE.begin() as connection:
        if replace_by_date and inspect(connection).has_table(table_name):
            values = ", ".join(df["date"].map(lambda x: f"'{x}'").unique())
            connection.execute(
                text(f"delete from {table_name} where date in ({values})")
            )

        return df.to_sql(
            table_name,
            con=connection,
            if_exists="replace" if replace else "append",
            index=False,
            dtype=__create_dtypes(df, dtype),
        )


def get_latest_date(table_name: str, date_column: str = "date") -> pd.Timestamp | None:
    if inspect(__ENGINE).has_table(table_name):
        max_date = pd.read_sql_query(
            f"select max({date_column}) date from {table_name}", con=__ENGINE
        )["date"]
        return max_date[0] if len(max_date) else None

    return None


def get_all_dates(table_name: str) -> list[pd.Timestamp]:
    if inspect(__ENGINE).has_table(table_name):
        return pd.read_sql_query(
            f"select distinct(date) date from {table_name} order by 1", con=__ENGINE
        )["date"].to_list()

    return []


def get_unprocessed_raw_data(
    table_name: str,
    raw_table_name: str,
    max_date: pd.Timestamp = None,
) -> pd.DataFrame:
    min_date = get_latest_date(table_name) or pd.Timestamp.min

    sql = f"SELECT * FROM {raw_table_name} WHERE date > '{min_date}'"

    if max_date:
        sql += f" and date < '{max_date}'"

    df = pd.read_sql(sql, con=__ENGINE)

    return df
