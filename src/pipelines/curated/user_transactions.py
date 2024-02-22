import logging
import pandas as pd
from utils import Tables, sql, Types
from data import indexer_data

__TRANSACTION_TYPES = [
    "Invalid",
    "Supply",
    "Borrow",
    "Withdraw",
    "Repay",
    "Repay(Auto)",
    "Liquidation",
    "Transfer(Received)",
    "Transfer(Sent)",
]


def __map_user_transactions(raw: pd.DataFrame) -> pd.DataFrame:
    raw["tx_type"] = raw["tx_type"].astype(int).map(lambda x: __TRANSACTION_TYPES[x])

    return raw


def run_curated_user_transactions_pipeline():
    logging.info("Running User Transactions pipeline")

    raw = indexer_data.get_all_user_transactions()
    logging.info(f"Found {len(raw)} user transaction records to process")

    df = __map_user_transactions(raw)
    dtype = {
        "tx_hash": Types.String(128),
        "tx_type": Types.String(18),
    }

    sql.save(df, Tables.USER_TRANSACTIONS, dtype=dtype, replace=True)
    logging.info(f"Successfully saved {len(df)} user transactions into curated DB")
