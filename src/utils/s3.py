import awswrangler as wr
from typing import Iterable
import boto3
import warnings

import logging
import pandas as pd

from config import AWS_REGIONS, S3_BUCKET

warnings.simplefilter(action="ignore", category=FutureWarning)
logging.getLogger("botocore").setLevel(logging.ERROR)

boto3_session = boto3.Session(region_name=AWS_REGIONS)


def read_partition(table_name: str, date: pd.Timestamp) -> pd.DataFrame:
    data = wr.s3.read_parquet(
        path=__build_s3_path(table_name, date), boto3_session=boto3_session
    )

    data.insert(0, "date", date)

    return data


def save(df: pd.DataFrame, table_name: str) -> int:
    dates = df["date"].unique()

    for date in dates:
        partition = df[df["date"] == date].drop(columns=["date"])
        save_partition(partition, table_name, date)

    return len(df)


def save_partition(df: pd.DataFrame, table_name: str, date: pd.Timestamp) -> int:
    assert "date" not in df.columns, "date column should not be in the DataFrame"
    assert isinstance(date, pd.Timestamp), "date column should be datetime"
    assert date.minute == 0, "date minutes should be 0"

    path = __build_s3_path(table_name, date)

    wr.s3.delete_objects(path, boto3_session=boto3_session)
    wr.s3.to_parquet(
        df=df,
        path=f"{__build_s3_path(table_name, date)}data.snappy.parquet",
        index=False,
        boto3_session=boto3_session,
    )

    return len(df)


def get_all_dates(
    table_name: str,
    min_date: pd.Timestamp = None,
    max_date: pd.Timestamp = None,
) -> list[pd.Timestamp]:
    directories = wr.s3.list_directories(
        f"s3://{S3_BUCKET}/{table_name}/", boto3_session=boto3_session
    )

    dates: Iterable[list] = map(__parse_date_from_directory, directories)

    if min_date:
        dates = filter(lambda x: x > min_date, dates)

    if max_date:
        dates = filter(lambda x: x < max_date, dates)

    return list(sorted(dates))


def get_latest_date(table_name: str) -> pd.Timestamp:
    all_dates = get_all_dates(table_name)

    return max(get_all_dates(table_name)) if all_dates else pd.Timestamp.min


def __parse_date_from_directory(directory: str) -> pd.Timestamp:
    paths = directory.split("/")
    assert paths[-1] == "", "last path should be empty"
    assert paths[-2].startswith("date="), "last folder name should start with date="

    return pd.Timestamp(paths[-2].split("=")[-1])


def __build_s3_path(table_name: str, date: pd.Timestamp) -> str:
    return f"s3://{S3_BUCKET}/{table_name}/date={date.strftime('%Y-%m-%d %H:00:00')}/"
