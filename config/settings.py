from decouple import config

openfigi_tokens_cast = lambda x: x.replace(" ", "").split(",")


LOG_LEVEL = config("LOG_LEVEL", default="INFO")
OUTPUT_TABLE = config("OUTPUT_TABLE")
BRIGHTDATA_PROXY = config("BRIGHTDATA_PROXY", cast=openfigi_tokens_cast)
BRIGHTDATA_PORT = config("BRIGHTDATA_PORT", cast=int)
BRIGHTDATA_USER = config("BRIGHTDATA_USER")
BRIGHTDATA_PASSWD = config("BRIGHTDATA_PASSWD")
OPENFIGI_TOKENS = config("OPENFIGI_TOKENS", cast=openfigi_tokens_cast)
OPENFIGI_MAX_RETRIES = config("OPENFIGI_MAX_RETRIES", cast=int, default=3)
OPENFIGI_BACKOFF_FACTOR = config("OPENFIGI_BACKOFF_FACTOR", cast=int, default=2)
MSSQL_AD_LOGIN = config("MSSQL_AD_LOGIN", cast=bool, default=False)
MSSQL_SERVER = config("MSSQL_SERVER")
MSSQL_DATABASE = config("MSSQL_DATABASE")

if not MSSQL_AD_LOGIN:
    MSSQL_USERNAME = config("MSSQL_USERNAME")
    MSSQL_PASSWORD = config("MSSQL_PASSWORD")
