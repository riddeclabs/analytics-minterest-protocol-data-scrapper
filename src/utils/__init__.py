from . import env, s3
from .data_fetcher import DataFetcher
from .types import Types


class Tables:
    RAW_MARKETS = "raw_markets"
    MARKETS_HISTORY = "markets_history"
    MARKETS_LATEST = "markets_latest"

    RAW_USERS = "raw_users"
    RAW_USERS_LATEST = "raw_users_latest"
    USERS_HISTORY = "users_history"
    USERS_LATEST = "users_latest"

    USER_MARKETS_HISTORY = "user_markets_history"
    USER_MARKETS_LATEST = "user_markets_latest"

    RAW_ORACLE_PRICES = "raw_oracle_prices"
    ORACLE_PRICES = "oracle_prices"

    USER_TRANSACTIONS = "user_transactions"
    NFT_TRANSACTIONS = "nft_transactions"
    LIQUIDATIONS = "liquidations"

    PIPELINES_STATUS = "pipelines_status"
