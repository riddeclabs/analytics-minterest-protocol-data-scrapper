import awswrangler as wr
from typing import Iterable
import boto3
import warnings

from tqdm.auto import tqdm
import logging
import pandas as pd

from config import AWS_REGIONS, ATHENA_BUCKET, ATHENA_DB

warnings.simplefilter(action="ignore", category=FutureWarning)
logging.getLogger("botocore").setLevel(logging.ERROR)

boto3_session = boto3.Session(region_name=AWS_REGIONS)


def read(sql: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql=sql,
        database=ATHENA_DB,
        boto3_session=boto3_session,
        ctas_approach=False,
    )


def read_partitions(
    table_name: str,
    min_date: pd.Timestamp = None,
    max_date: pd.Timestamp = None,
) -> tuple[list[pd.Timestamp], Iterable[tuple[pd.Timestamp, pd.DataFrame]]]:
    all_dates = get_all_dates(table_name, min_date, max_date)

    def __create_generator() -> Iterable[tuple[pd.Timestamp, pd.DataFrame]]:
        for hour in (progress := tqdm(all_dates, desc=table_name)):
            partition = read_partition(table_name, hour)
            progress.set_description(f"{table_name}: {hour} ({len(partition)} records)")

            yield hour, read_partition(table_name, hour)

    return all_dates, __create_generator()


def read_partition(table_name: str, date: pd.Timestamp) -> pd.DataFrame:
    return wr.s3.read_parquet(
        path=f"s3://{ATHENA_BUCKET}/{table_name}/date={date.strftime('%Y-%m-%d %H:00:00')}/",
        dataset=True,
    )


def save(df: pd.DataFrame, table_name: str) -> int:
    assert "date" in df.columns, "date column is required"
    assert df["date"].dtype == "datetime64[ns]", "date column should be datetime"
    assert df["date"].dt.minute.eq(0).all(), "date minutes should be 0"

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{ATHENA_BUCKET}/{table_name}/",
        dataset=True,
        index=False,
        database=ATHENA_DB,
        table=table_name,
        partition_cols=["date"],
        schema_evolution=True,
        mode="overwrite_partitions",
        boto3_session=boto3_session,
    )

    return len(df)


def get_all_dates(
    table_name: str,
    min_date: pd.Timestamp = None,
    max_date: pd.Timestamp = None,
) -> list[pd.Timestamp]:
    if not wr.catalog.does_table_exist(
        database=ATHENA_DB, table=table_name, boto3_session=boto3_session
    ):
        return []

    partitions = wr.catalog.get_partitions(
        database=ATHENA_DB, table=table_name, boto3_session=boto3_session
    )

    flattened = set(
        [pd.Timestamp(item) for sublist in partitions.values() for item in sublist]
    )

    if min_date:
        flattened = filter(lambda x: x > min_date, flattened)

    if max_date:
        flattened = filter(lambda x: x < max_date, flattened)

    return list(sorted(flattened))


def get_latest_date(table_name: str) -> pd.Timestamp:
    all_dates = get_all_dates(table_name)

    return max(get_all_dates(table_name)) if all_dates else pd.Timestamp.min
