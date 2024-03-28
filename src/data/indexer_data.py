import pandas as pd
from sqlalchemy import create_engine


from config import INDEXER_DB_SQL_CONNECTION_STRING

LIQUIDATIONS_DB_SQL_CONNECTION_STRING = INDEXER_DB_SQL_CONNECTION_STRING.replace("indexer_", "inquisition_")

__ENGINE = create_engine(INDEXER_DB_SQL_CONNECTION_STRING)


def get_all_user_addresses() -> list[str]:
    df = pd.read_sql(
        "SELECT DISTINCT user_address FROM user_market_state", con=__ENGINE
    )

    return df["user_address"].to_list()


def get_all_user_transactions() -> pd.DataFrame:
    df = pd.read_sql(
        """
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
        """
            select 
                to_timestamp(br.timestamp) at time zone 'utc' date,
                nft.id,
                nft.owner,
                nft.amount,
                case 
                    when (row_number() over(partition by nft.id, nft.owner order by nft.block desc) = 1) then true
                    else false
                end as is_latest
            from nft_ownership_state nft
            join block_record br on nft.block = br.number
            order by date
        """,
        con=__ENGINE,
    )

    return df


def get_all_liquidations() -> pd.DataFrame:
    df = pd.read_sql(
        """
            with market_info as (
                select * 
                from (values
                    ('0x66DBC77A4E6F3290493894Fc8e18F91C4F0ca854', 'Minterest Tether USD', 'musdt', 8, '0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE', 6),
                    ('0xEcbE4A2519f1e26df8Dfde95a4a2b89DE832896C', 'Minterest USD Coin', 'musdc', 8, '0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9', 6),
                    ('0xfa1444aC7917d6B96Cac8307E97ED9c862E387Be', 'Minterest Wrapped Ether', 'mweth', 8, '0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111', 18),
                    ('0x6Cc1560EFe633E8799226c87c45981ef93cFa617', 'Minterest Wrapped MNT', 'mwmnt', 8, '0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8', 18),
                    ('0x5edBD8808F48Ffc9e6D4c0D6845e0A0B4711FD5c', 'Minterest Ondo U.S. Dollar Yield', 'musdy', 8, '0x5bE26527e817998A7206475496fDE1E68957c5A6', 18),
                    ('0x5aA322875a7c089c1dB8aE67b6fC5AbD11cf653d', 'Minterest Mantle Staked Ether', 'mmeth', 8, '0xcDA86A272531e8640cD7F1a92c01839911B90bb0', 18)
                ) as t(address, name, symbol, market_decimals, underlying_address, underlying_decimals)
            )
            select 
                le.liquidation_timestamp as date,
                le.id as liquidation_id,
                le.transaction_hash as tx_hash,
                sm.symbol as seize_market,
                seize_amount_underlying / pow(10, sm.underlying_decimals) as seize_amount,
                rm.symbol as repay_market,
                repay_amount_underlying / pow(10, rm.underlying_decimals) as repay_amount,
                le.borrower_address,
                le.is_debt_healthy,
                la.est_max_fee_per_gas_wei,
                la.est_max_priority_fee_per_gas_wei,
                la.est_raw_profit_usd / 10e17 as est_raw_profit_usd,
                la.est_net_profit_usd / 10e17 as est_net_profit_usd,
                la.actual_net_profit_usd / 10e17 as actual_net_profit_usd,
                la.est_gas_used_liquidation,
                la.actual_gas_used_liquidation,
                la.est_gas_fee_liquidation_usd / 10e17 as est_gas_fee_liquidation_usd,
                la.actual_gas_price_wei,
                la.actual_gas_fee_liquidation_eth / 10e17 as actual_gas_fee_liquidation_tokens,
                la.actual_gas_fee_liquidation_usd / 10e17 as actual_gas_fee_liquidation_usd,
                la.eth_price_usd / 10e17 as eth_price_usd,
                la.healthy_factor_before / 10e17 as healthy_factor_before,
                la.healthy_factor_after / 10e17 as healthy_factor_after
            from liquidation_event le
            left join liquidation_accounting la using(transaction_hash)
            join market_info sm on le.seize_market = sm.address
            join market_info rm on le.repay_market = rm.address
        """,
        con=LIQUIDATIONS_DB_SQL_CONNECTION_STRING,
    )

    return df
