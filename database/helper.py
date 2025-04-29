from database import MSSQLDatabase


def init_db_instance():
    return MSSQLDatabase()


def get_ishares():
    conn = init_db_instance()
    query = """
    SELECT sub.*
    FROM (
        SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ISIN, ishares_exchange_name
            ORDER BY (SELECT NULL)
            -- if you have a “latest” column, e.g. load_date, use:
            -- ORDER BY load_date DESC
        ) AS rn
        FROM etl.ishares_index_components
        WHERE ishares_index IN (
        'iShares Core MSCI International Developed Markets ETF',
        'iShares Russell 3000 ETF',
        'iShares Core S&P Total U.S. Stock Market ETF',
        'iShares MSCI Germany ETF',
        'iShares MSCI Germany Small-Cap ETF',
        'iShares Core DAX® UCITS ETF (DE)',
        'iShares MDAX® UCITS ETF (DE)',
        'iShares Core SPI® ETF (CH)',
        'iShares Core MSCI Total International Stock ETF',
        'iShares Core MSCI Japan IMI UCITS ETF',
        'iShares MSCI Global Metals & Mining Producers ETF',
        'iShares MSCI World Islamic UCITS ETF',
        'iShares MSCI World ETF',
        'iShares MSCI Emerging Markets ETF'
        )
    ) AS sub
    WHERE sub.rn = 1;
    """
    table = conn.select_table(query)
    return table


def get_exchanges(comp=False):
    conn = init_db_instance()
    if comp:
        query = """
        SELECT ishares_exchange_name, exchange_priority, bbg_exch_comp
        FROM mdd.Exchanges
        WHERE ishares_exchange_name IS NOT NULL
        """
    else:
        query = """
        SELECT ishares_exchange_name, exchange_priority, bbg_exch
        FROM mdd.Exchanges
        WHERE ishares_exchange_name IS NOT NULL
        """

    table = conn.select_table(query)
    records = [list(data.values()) for data in table.to_dict("records")]
    return records
