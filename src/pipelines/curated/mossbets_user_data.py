import logging
import pandas as pd

from utils import Tables, sql, google_sheets, formatting, Types
from config import MOSSBETS_USER_DATA_GOOGLE_SHEETS_ID

SHEETS_TO_SAVE = ["Raw data", "New data"]
STRING_COLUMNS = ["month", "week", "date"]


def run_curated_mossbets_user_data_google_sheets_export_pipeline():
    logging.info("Running Mossbets User Data Google Sheets export pipeline")

    data = google_sheets.read_all_data(MOSSBETS_USER_DATA_GOOGLE_SHEETS_ID)
    logging.info(f"Found {len(data)} pages in the Mossbets User Data spreadsheet")

    for sheet_name in SHEETS_TO_SAVE:
        df = data[sheet_name]
        df.columns = [formatting.to_snake_case(column) for column in df.columns]
        for column in df.columns:
            df[column] = (
                df[column]
                .replace({",": "", "€": "", "%": ""}, regex=True)
                .astype(float, errors="ignore")
            )

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        sql.save(
            df,
            formatting.to_snake_case(sheet_name),
            schema=Tables.MOSSBETS_SCHEMA_NAME,
            replace=True,
            default_str_length=256,
            dtype={"date": Types.Date},
        )

        logging.info(
            f"Successfully saved {len(df)} records from the '{sheet_name}' sheet"
        )
