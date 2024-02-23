import logging
from utils import Tables, sql, Types
from data import indexer_data


def run_curated_nft_transactions_pipeline():
    logging.info("Running NFT Transactions pipeline")

    raw = indexer_data.get_all_nft_transactions()
    logging.info(f"Found {len(raw)} user transaction records to process")

    dtype = {
        "id": Types.Int,
    }

    sql.save(raw, Tables.NFT_TRANSACTIONS, dtype=dtype, replace=True)
    logging.info(f"Successfully saved {len(raw)} NFT transactions into curated DB")
