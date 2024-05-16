import logging

from utils import Tables, Types, sql, google_sheets
from config import MINTEREST_NFT_OVERVIEW_GOOGLE_SHEETS_ID


def run_curated_nft_tiers_pipeline():
    logging.info("Running NFT Tiers pipeline")

    all_sheets = google_sheets.read_all_data(MINTEREST_NFT_OVERVIEW_GOOGLE_SHEETS_ID)
    nft_tiers = list(all_sheets.values())[0]
    logging.info(f"Found {len(nft_tiers)} NFT tier records to process")

    dtype = {
        "id_1": Types.Int,
        "id_2": Types.Int,
        "id_3": Types.Int,
    }

    sql.save(nft_tiers, Tables.NFT_TIERS, dtype=dtype, replace=True)
    logging.info(f"Successfully saved {len(nft_tiers)} NFT tiers into curated DB")
