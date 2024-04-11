import logging
import pandas as pd
from utils import Tables, sql, Types
from data import indexer_data

__VESTING_REFUND_CHOICES = [
    "Not Selected",
    "Stack",
    "Skip",
]


def __map_vesting_refund_poll_records(raw: pd.DataFrame) -> pd.DataFrame:
    raw["choice"] = raw["choice"].astype(int).map(lambda x: __VESTING_REFUND_CHOICES[x])

    return raw


def run_curated_vesting_refund_poll_pipeline():
    logging.info("Running Vesting Refund Poll pipeline")

    raw = indexer_data.get_all_vesting_refund_poll_records()
    logging.info(f"Found {len(raw)} vesting refund poll records to process")

    df = __map_vesting_refund_poll_records(raw)
    dtype = {
        "signed_message": Types.String(1024),
    }

    sql.save(df, Tables.VESTING_REFUND_POLL, dtype=dtype, replace=True)
    logging.info(f"Successfully saved {len(df)} vesting refund poll records into curated DB")
