import pandas as pd
from sqlalchemy import create_engine

from config import INDEXER_DB_SQL_CONNECTION_STRING


__ENGINE = create_engine(INDEXER_DB_SQL_CONNECTION_STRING)


def get_all_user_addresses() -> list[str]:
    df = pd.read_sql(
        "SELECT DISTINCT user_address FROM user_market_state", con=__ENGINE
    )

    return df["user_address"].to_list()


def get_all_user_transactions() -> pd.DataFrame:
    df = pd.read_sql(
        f"""
            select 
                er.user_address,
                to_timestamp(br.timestamp) at time zone 'utc' date,
                er.tx_hash,
                er.type tx_type,
                mi.symbol market_symbol,
                ae.amount / pow(10, mi.underlying_decimals) amount,
                round(ae.amount_usd / 10e17, 3) amount_usd
            from event_record er
            join block_record br on br.number = er.block
            join market_info mi on mi.address = er.contract_address
            join event_record_amount_ext ae on ae.event_id = er.id
            order by er.block, er.user_address
        """,
        con=__ENGINE,
    )

    return df


def get_all_nft_transactions() -> pd.DataFrame:
    df = pd.read_sql(
        f"""
            select 
                to_timestamp(br.timestamp) at time zone 'utc' date,
                nft.id,
                nft.owner,
                nft.amount,
                case 
                    when (count(nft.owner) over(partition by nft.id order by nft.block desc) = 1) then true
                    else false
                end as is_latest
            from nft_ownership_state nft
            join block_record br on nft.block = br.number
            order by date
        """,
        con=__ENGINE,
    )

    return df
