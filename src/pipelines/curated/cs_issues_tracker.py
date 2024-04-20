import logging

from utils import Tables, sql, google_sheets, formatting
from config import CS_ISSUES_TRACKER_GOOGLE_SHEETS_ID


def run_curated_cs_issues_tracker_google_sheets_export_pipeline():
    logging.info("Running CS Issues Tracker Google Sheets export pipeline")

    data = google_sheets.read_all_data(CS_ISSUES_TRACKER_GOOGLE_SHEETS_ID)
    logging.info(f"Found {len(data)} pages in the CS Issues Tracker spreadsheet")

    for sheet_name, df in data.items():
        df.columns = [formatting.to_snake_case(column) for column in df.columns]

        sql.save(
            df,
            formatting.to_snake_case(sheet_name),
            schema=Tables.CS_ISSUES_TRACKER_SCHEMA_NAME,
            replace=True,
            default_str_length=512,
        )

        logging.info(
            f"Successfully saved {len(df)} records from the '{sheet_name}' sheet"
        )
