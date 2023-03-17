from sqlalchemy import types

from . import env, sql


class Tables:
    RAW_MARKETS = "raw_markets"
    MARKETS_HISTORY = "markets_history"
    MARKETS_LATEST = "markets_latest"

    RAW_USERS = "raw_users"
    USERS_HISTORY = "users_history"
    USERS_LATEST = "users_latest"

    USER_MARKETS_HISTORY = "user_markets_history"
    USER_MARKETS_LATEST = "user_markets_latest"

    RAW_ORACLE_PRICES = "raw_oracle_prices"
