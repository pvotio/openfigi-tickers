from config import logger, settings
from database import MSSQLDatabase


def init_db_instance():
    return MSSQLDatabase()


def get_ishares():
    conn = init_db_instance()
    query = settings.ISHARES_QUERY
    table = conn.select_table(query)
    table.drop(columns=["timestamp_created_utc", "rn"], inplace=True)
    return table


def get_exchanges_priority(comp=False):
    conn = init_db_instance()
    if comp:
        query = settings.EXCHANGES_COMP_QUERY
    else:
        query = settings.EXCHANGES_QUERY

    table = conn.select_table(query)
    records = [list(data.values()) for data in table.to_dict("records")]
    return records


def get_all_exchanges():
    conn = init_db_instance()
    query = """
    SELECT
        ishares_exchange_name,
        ext1_exch,
        ext2_exch,
        ext3_exch,
        bbg_exch,
        bbg_exch_comp,
        country_iso2,
        ext2_exch_comp,
        ext3_exch_comp,
        ext8_exch_comp
    FROM mdd.Exchanges
    WHERE ishares_exchange_name IS NOT NULL
    """

    try:
        table = conn.select_table(query)
        result = {}

        for row in table.to_dict("records"):
            exch_name = row["ishares_exchange_name"]
            exch_info = {
                "12data": row["ext1_exch"],
                "eod": row["ext2_exch"],
                "yahoo": row["ext3_exch"],
                "bbg_exch": row["bbg_exch"],
                "bbg_exch_comp": row["bbg_exch_comp"],
                "country_iso2": row["country_iso2"],
                "ext2_exch_comp": row["ext2_exch_comp"],
                "ext3_exch_comp": row["ext3_exch_comp"],
                "ext8_exch_comp": row["ext8_exch_comp"],
            }

            if exch_name not in result:
                result[exch_name] = []

            result[exch_name].append(exch_info)

        return result

    except Exception as e:
        logger.error(f"Failed to fetch all exchanges: {e}")
        return {}


def get_currencies():
    conn = init_db_instance()
    query = """
    select ishares_exchange_name, currency_country from mdd.Exchanges
    """
    table = conn.select_table(query)
    records = {
        list(data.values())[0]: list(data.values())[1]
        for data in table.to_dict("records")
    }
    return records


def get_eod_tickers():
    conn = init_db_instance()
    query = """
    select isin, ext2_ticker from etl.eodhd_tickers where ext2_ticker is not null
    """
    table = conn.select_table(query)
    tickers = list(table.to_dict("list").values())[1]
    isin_ticker_map = {}
    for record in table.to_dict("records"):
        isin, ticker = list(record.values())
        if isin not in isin_ticker_map:
            isin_ticker_map[isin] = []

        isin_ticker_map[isin].append(ticker)

    return tickers, isin_ticker_map
