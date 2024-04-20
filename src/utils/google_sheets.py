from googleapiclient.discovery import build, Resource
import pandas as pd
from functools import cache
from google.oauth2.service_account import Credentials

from config import GOOGLE_CLOUD_CREDENTIALS


@cache
def __get_service() -> Resource:
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_info(
        GOOGLE_CLOUD_CREDENTIALS, scopes=scopes
    )

    return build("sheets", "v4", credentials=credentials)


def __read_sheet(spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    sheet = __get_service().spreadsheets()

    result = (
        sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    )
    values = result.get("values", [])

    columns = values[0]
    rows = values[1:]

    rows_length = max([len(row) for row in rows])
    column_names = [
        (
            columns[index]
            if index < len(columns)
            else f"Unnamed_{index - len(columns) + 1}"
        )
        for index in range(rows_length)
    ]

    return pd.DataFrame(values[1:], columns=column_names)


def read_all_data(spreadsheet_id: str) -> dict[str, pd.DataFrame]:
    sheet = __get_service().spreadsheets()

    result = sheet.get(spreadsheetId=spreadsheet_id).execute()
    sheets = result.get("sheets", [])

    return {
        sheet["properties"]["title"]: __read_sheet(
            spreadsheet_id, sheet["properties"]["title"]
        )
        for sheet in sheets
    }
