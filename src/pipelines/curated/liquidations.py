import logging

from data import indexer_data
from utils import Tables, Types, sql


def run_curated_liquidations_pipeline():
    logging.info("Running Liquidations pipeline")

    raw = indexer_data.get_all_liquidations()
    logging.info(f"Found {len(raw)} liquidation records to process")

    dtype = {
        "tx_hash": Types.String(128),
    }

    sql.save(raw, Tables.LIQUIDATIONS, dtype=dtype, replace=True)
    logging.info(f"Successfully saved {len(raw)} liquidations into curated DB")
