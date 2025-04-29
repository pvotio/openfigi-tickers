from config import settings
from database import MSSQLDatabase


def init_db_instance():
    return MSSQLDatabase()


def get_ishares():
    conn = init_db_instance()
    query = settings.ISHARES_QUERY
    table = conn.select_table(query)
    return table


def get_exchanges(comp=False):
    conn = init_db_instance()
    if comp:
        query = settings.EXCHANGES_COMP_QUERY
    else:
        query = settings.EXCHANGES_QUERY

    table = conn.select_table(query)
    records = [list(data.values()) for data in table.to_dict("records")]
    return records
