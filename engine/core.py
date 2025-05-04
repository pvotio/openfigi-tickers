import pandas as pd

from config import logger
from database.helper import (
    get_all_exchanges,
    get_currencies,
    get_eod_tickers,
    get_exchanges_priority,
    get_ishares,
)
from engine.openfigi import OpenFIGI


class Core:

    def __init__(self):
        logger.info("Initializing Core")
        self.eod_exch_index = {}
        self.load_db_data()

    def run(self):
        ofg = OpenFIGI(self.ishares, self.exchanges_priority, keep_unlisted=True)
        logger.info("Running OpenFIGI for primary exchanges")
        result = ofg.run()
        logger.info(f"Received {len(result)} results from primary OpenFIGI")

        unmatched_records = self.get_unmatched_records(result)
        logger.info(f"Found {len(unmatched_records)} unmatched records")

        ofg_comp = OpenFIGI(
            unmatched_records, self.exchanges_priority_comp, keep_unlisted=False
        )
        logger.info("Running OpenFIGI for component exchanges")
        result_comp = ofg_comp.run()
        logger.info(f"Received {len(result_comp)} results from component OpenFIGI")

        self.result_combined = self.combine_opnefigi_results(result, result_comp)
        logger.info(f"Combined total result count: {len(self.result_combined)}")

        self._generate_tickers()
        logger.info("Generated tickers for combined results")

        self._add_exchange()
        logger.info("Add exchange data to records")

        self.dataframe = pd.DataFrame(self.result_combined)
        logger.info("Core.run() complete")
        return self.dataframe

    def load_db_data(self):
        logger.info("Loading data from database")
        self.ishares = get_ishares()
        logger.debug(f"Loaded {len(self.ishares)} ishares records")

        self.currencies = get_currencies()
        logger.debug(f"Loaded {len(self.currencies)} currency mappings")

        self.eod_tickers_list, self.isin_eod_tickers_map = get_eod_tickers()
        logger.debug(f"Loaded {len(self.eod_tickers_list)} EOD tickers")

        self.exchanges = get_all_exchanges()
        logger.debug(f"Loaded {len(self.exchanges)} exchange groups")

        self.exchanges_priority = self.get_exchanges()
        logger.debug(
            f"Built exchange priority map with {len(self.exchanges_priority)} entries"
        )

        self.exchanges_priority_comp = self.get_exchanges(comp=True)
        logger.debug(
            f"Built complementary exchange priority map with {len(self.exchanges_priority_comp)} entries"  # noqa: E501
        )

    def _add_exchange(self):
        logger.info("Adding exchange metadata to result records")
        for row in self.result_combined:
            if "exchCode" not in row:
                return row

            if row["ishares_exchange_name"] not in self.exchanges:
                return row

            for exch in self.exchanges[row["ishares_exchange_name"]]:
                if exch["bbg_exch"] in row["exchCode"]:
                    exchange = exch
                    break
                elif exch["bbg_exch_comp"] in row["exchCode"]:
                    exchange = exch
                    break

            for k, v in exchange.items():
                row[k] = v

            if row["OPENFIGI Ticker"] == row["OPENFIGI COMP Ticker"]:
                row["bbg_exch"] = row["bbg_exch_comp"]

    def _generate_tickers(self):
        logger.info("Generating tickers for each result row")
        for idx, row in enumerate(self.result_combined):
            logger.debug(f"Generating tickers for row {idx}")
            for func in [
                self._generate_eod_ticker,
                (self._generate_eod_ticker,),
                self._generate_yahoo_ticker,
                (self._generate_yahoo_ticker,),
                self._generate_openfigi_ticker,
            ]:
                if isinstance(func, tuple):
                    resp = func[0](row, comp=True)
                else:
                    resp = func(row)

                if not resp:
                    continue

                for k, v in resp.items():
                    if k not in row:
                        row[k] = v

    def _generate_eod_ticker(self, row, comp=False):
        if "exchCode" not in row:
            return None

        if row["ishares_exchange_name"] not in self.exchanges:
            return None

        result = []

        for exch in self.exchanges[row["ishares_exchange_name"]]:
            if exch["bbg_exch"] in row["exchCode"]:
                exchange = exch
                break
            elif exch["bbg_exch_comp"] in row["exchCode"]:
                exchange = exch
                break

        if comp:
            _exchcode = exchange["ext2_exch_comp"]
        else:
            _exchcode = exchange["eod"]

        if not _exchcode:
            return None

        if "," in _exchcode:
            _exchcode = _exchcode.replace(" ", "").split(",")
        else:
            _exchcode = [_exchcode]

        for eod_exchcode in _exchcode:
            ticker = str(row["exchange_ticker"])
            securitydesc = str(row["securityDescription"])
            if "/" in securitydesc and "." not in ticker:
                base = __base = securitydesc
                base = base.replace("/", "-")

                for _ in [".R", ".E"]:
                    if _ in base:
                        base = base.replace(_, "")

                base = f"{base}.{eod_exchcode}"
                result.append(base)
            else:
                base = __base = ticker

                if eod_exchcode == "HK":
                    if base.isdigit():
                        if len(base) < 4:
                            base = "".join(["0" for _ in range(4 - len(base))]) + base

                elif eod_exchcode in ["KO", "KQ", "SHG", "SHE"]:
                    if base.isdigit():
                        if len(base) < 6:
                            base = "".join(["0" for _ in range(6 - len(base))]) + base

                for _ in [".R", ".E"]:
                    if _ in base:
                        base = base.replace(_, "")

                if base[-1] == ".":
                    base = base.replace(".", "")
                else:
                    base = base.replace(".", "-")

                base = base.replace("*", "")
                if " " in base:
                    base = base.replace(" ", "-")

                base = f"{base}.{eod_exchcode}"
                result.append(base)

        if len(result) == 0:
            return None

        for i in result:
            if i in self.eod_tickers_list:
                if "." in i:
                    exch = i.split(".")[1]
                    self.eod_exch_index[__base] = _exchcode.index(exch)

                if comp:
                    return {"ext2_comp_ticker": i}
                else:
                    return {"EOD Ticker": i}
            else:
                tickers = self.get_eod_ticker_by_isin(
                    row["isin"].replace(" ", "").strip()
                )
                if len(tickers) > 0:
                    for _ in _exchcode:
                        for t in tickers:
                            if _ in t:
                                if comp:
                                    return {"ext2_comp_ticker": t}
                                else:
                                    return {"EOD Ticker": t}

    def _generate_yahoo_ticker(self, row, comp=False):
        if "exchCode" not in row:
            return None

        if row["ishares_exchange_name"] not in self.exchanges:
            return None

        for exch in self.exchanges[row["ishares_exchange_name"]]:
            if exch["bbg_exch"] in row["exchCode"]:
                exchange = exch
                break
            elif exch["bbg_exch_comp"] in row["exchCode"]:
                exchange = exch
                break

        if comp:
            yahoo_exchcode = exchange["ext3_exch_comp"]
        else:
            yahoo_exchcode = exchange["yahoo"]

        if not yahoo_exchcode:
            return None

        ticker = str(row["exchange_ticker"])
        securitydesc = str(row["securityDescription"])

        if "," in yahoo_exchcode:
            yahoo_exchcode = yahoo_exchcode.replace(" ", "").split(",")

            if ticker in self.eod_exch_index:
                indx = self.eod_exch_index[ticker]
                yahoo_exchcode = yahoo_exchcode[indx]
            elif securitydesc in self.eod_exch_index:
                indx = self.eod_exch_index[securitydesc]
                yahoo_exchcode = yahoo_exchcode[indx]
            else:
                yahoo_exchcode = yahoo_exchcode[0]

        if "/" in securitydesc and "." not in ticker:
            base = securitydesc
            base = base.replace("/", "-")
            if yahoo_exchcode != "US":
                base = f"{base}.{yahoo_exchcode}"

            if comp:
                return {"ext3_comp_ticker": base}
            else:
                return {"Yahoo Ticker": base}
        else:
            base = ticker
            if yahoo_exchcode == "HK":
                if base.isdigit():
                    if len(base) < 4:
                        base = "".join(["0" for _ in range(4 - len(base))]) + base

            elif yahoo_exchcode == "KO" or yahoo_exchcode == "SHG":
                if base.isdigit():
                    if len(base) < 6:
                        base = "".join(["0" for _ in range(6 - len(base))]) + base

            if base[-1] == ".":
                base = base.replace(".", "")
            else:
                base = base.replace(".", "-")

            base = base.replace("*", "")
            if " " in base:
                if base[-2] == " ":
                    base = base.replace(" ", "-")
                elif base[-3] == " ":
                    base = base[:-3]

            if yahoo_exchcode != "US":
                base = f"{base}.{yahoo_exchcode}"

            if comp:
                return {"ext3_comp_ticker": base}
            else:
                return {"Yahoo Ticker": base}

    def _generate_openfigi_ticker(self, row):
        if "ticker" not in row:
            return False

        exch = row["exchCode"]
        if " " in exch:
            exch = exch.split(" ")[0]

        news = row["ticker"] + ":" + exch
        ticker = row["ticker"] + " " + exch + " " + row["marketSector"]
        ticker_comp = ""

        if row["ishares_exchange_name"] in self.exchanges:
            for exch in self.exchanges[row["ishares_exchange_name"]]:
                if (
                    exch["bbg_exch"] in row["exchCode"]
                    or exch["bbg_exch_comp"] in row["exchCode"]
                ):
                    ticker_comp = (
                        row["ticker"]
                        + " "
                        + exch["bbg_exch_comp"]
                        + " "
                        + row["marketSector"]
                    )

        return {
            "OPENFIGI News Ticker": news,
            "OPENFIGI Ticker": ticker,
            "OPENFIGI COMP Ticker": ticker_comp,
        }

    def get_eod_ticker_by_isin(self, isin):
        return self.isin_eod_tickers_map.get(isin, [])

    def get_currency_by_exch(self, ishare_exch):
        return self.currencies.get(ishare_exch, False)

    @staticmethod
    def combine_opnefigi_results(result_a, result_b):
        result = []
        for row in result_a:
            if "figi" not in row:
                continue

            result.append(row)

        for row in result_b:
            if row not in result:
                result.append(row)

        return result

    @staticmethod
    def get_unmatched_records(openfigi_records):
        result = []
        for openfigi_record in openfigi_records:
            if "figi" in openfigi_record:
                continue

            result.append(openfigi_record)

        return result

    @staticmethod
    def get_exchanges(comp=False):
        records = get_exchanges_priority(comp=comp)
        result = {}
        for record in records:
            ishare, priority, exch = record
            if ishare not in result:
                result[ishare] = {}

            if exch not in result[ishare].values():
                result[ishare][priority] = exch

        return result
