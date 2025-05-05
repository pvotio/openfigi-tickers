from datetime import datetime

import pandas as pd


class Transformer:
    def __init__(self, raw_df: pd.DataFrame):
        self.raw_df = raw_df.copy()

    @staticmethod
    def valcheck(value, only_null_check=False):
        if value in ["NaN", "", 0, None, "-"]:
            return None
        elif only_null_check:
            return value

        if isinstance(value, str):
            if value == "INF":
                return value
            try:
                return round(float(value), 4)
            except Exception:
                return value

        if isinstance(value, int):
            return round(float(value), 4)

        return value

    @staticmethod
    def timenow():
        return datetime.utcnow()

    def transform(self) -> pd.DataFrame:
        COLUMN_MAP = {
            "exchange_ticker": "exchange_ticker",
            "ishares_name": "ishares_name",
            "isin": "isin",
            "ishares_exchange_name": "ishares_exchange_name",
            "Market Currency": "currency",
            "cusip": "cusip",
            "sedol": "sedol",
            "figi": "bbg_figi",
            "name": "bbg_name",
            "compositeFIGI": "bbg_compositefigi",
            "securityType": "bbg_securitytype",
            "marketSector": "bbg_marketsector",
            "securityType2": "bbg_securitytype2",
            "securityDescription": "bbg_securitydescription",
            "shareClassFIGI": "bbg_shareclassfigi",
            "EOD Ticker": "ext2_ticker",
            "Yahoo Ticker": "ext3_ticker",
            "OPENFIGI Ticker": "bbg_ticker",
            "OPENFIGI COMP Ticker": "bbg_comp_ticker",
            "ext2_comp_ticker": "ext2_comp_ticker",
            "ext3_comp_ticker": "ext3_comp_ticker",
            "ext8_comp_ticker": "ext8_comp_ticker",
            "bbg_exch": "bbg_exch",
            "bbg_exch_comp": "bbg_exch_comp",
            "country_iso2": "country_iso2",
            "wkn": "wkn",
            "valor": "valor",
        }

        cleaned_rows = []

        for _, row in self.raw_df.iterrows():
            cleaned_row = {}

            for raw_col, final_col in COLUMN_MAP.items():
                if raw_col in row:
                    value = row[raw_col]
                    if raw_col in ["cusip", "sedol"]:
                        cleaned_row[final_col] = self.valcheck(
                            value, only_null_check=True
                        )
                    else:
                        cleaned_row[final_col] = self.valcheck(value)
                else:
                    cleaned_row[final_col] = None

            cleaned_row["timestamp_created_utc"] = self.timenow()
            cleaned_rows.append(cleaned_row)

        final_columns = list(COLUMN_MAP.values()) + ["timestamp_created_utc"]
        return pd.DataFrame(cleaned_rows, columns=final_columns)
