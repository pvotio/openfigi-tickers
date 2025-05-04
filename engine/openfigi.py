import random
import threading
import time

import pandas as pd
import requests

from config import logger, settings


class OpenFIGI:

    OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
    THREAD_COUNT = settings.OPENFIGI_THREAD_COUNT
    MAX_RETRIES = settings.OPENFIGI_MAX_RETRIES
    BACKOFF_FACTOR = settings.OPENFIGI_BACKOFF_FACTOR

    def __init__(self, ishares, exchanges, keep_unlisted=False):
        self.alive = True
        self.keep_unlisted = keep_unlisted
        self.ishares = ishares
        self.exchanges = exchanges
        self.tasks = []
        self.batch_tasks = []
        self.ishares_map = {}
        self.raw_openfigi_resp = []
        self.result = []

    def run(self):
        logger.info("Starting Openfigi run")
        self._create_tasks()
        logger.debug(f"Created {len(self.tasks)} tasks for processing")
        self.batch_tasks = self._create_batch(self.tasks, self.THREAD_COUNT)
        logger.info(f"Split tasks into {len(self.batch_tasks)} batches")
        self.start_threads()
        logger.info("All threads have completed")
        self._cleanup_duplicates()
        logger.info("Duplicates cleaned up")
        self._filter_exchange_pairs()
        logger.info("Exchange pairs filtered")
        self._assemble_final()
        logger.info(f"Final assembly complete with {len(self.result)} records")
        self.dataframe = pd.DataFrame(self.result)
        logger.info("Converted result to DataFrame")
        return list(self.result)

    def start_threads(self):
        threads = []
        for tasks in self.batch_tasks:
            if tasks:
                t = threading.Thread(target=self.worker, args=[tasks])
                threads.append(t)
                logger.debug(f"Starting thread {t.name} for batch of size {len(tasks)}")
                t.start()

        for t in threads:
            t.join()
            logger.debug(f"Thread {t.name} has finished")

    def worker(self, tasks):
        while tasks and self.alive:
            batch = tasks[:30]
            tasks[:] = tasks[len(batch):]
            requests_list = self._create_request_body(batch)
            body = [x["body"] for x in requests_list]

            if not body:
                continue

            responses = self._request_api(body)
            if not responses:
                continue

            for req, resp in zip(requests_list, responses):
                entry = {
                    "data": req["task"],
                    "response": resp.get("data", [{}])[0] if resp.get("data") else [],
                }
                if entry not in self.raw_openfigi_resp:
                    self.raw_openfigi_resp.append(entry)

    def _request_api(self, body, retry=0):
        headers = {
            "Content-Type": "application/json",
            "X-OPENFIGI-APIKEY": random.choice(settings.OPENFIGI_TOKENS),
        }
        try:
            proxies = {
                "http": f"http://{settings.BRIGHTDATA_USER}-session-{random.random()}:{settings.BRIGHTDATA_PASSWD}@{settings.BRIGHTDATA_PROXY}:{settings.BRIGHTDATA_PORT}",  # noqa: E501
                "https": f"https://{settings.BRIGHTDATA_USER}-session-{random.random()}:{settings.BRIGHTDATA_PASSWD}@{settings.BRIGHTDATA_PROXY}:{settings.BRIGHTDATA_PORT}",  # noqa: E501
            }
            logger.debug(
                f"Sending request to OpenFIGI with {len(body)} items, retry={retry}"
            )
            resp = requests.post(
                self.OPENFIGI_MAPPING_URL, headers=headers, json=body, proxies=proxies
            )
            if resp.status_code == 200:
                logger.debug("Received successful response from OpenFIGI API")
                return resp.json()
            else:
                logger.warning(f"Unexpected status {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error requesting OpenFIGI API: {e}")
            if retry < self.MAX_RETRIES:
                backoff = retry**self.BACKOFF_FACTOR
                logger.info(f"Retrying after {backoff} seconds (retry {retry + 1})")
                time.sleep(backoff)
                return self._request_api(body, retry + 1)
            logger.error("Max retries reached, giving up on this batch")
            return []

    def _create_tasks(self):
        if isinstance(self.ishares, pd.DataFrame):
            ishare_records = self.ishares.to_dict("records")
        elif isinstance(self.ishares, list):
            ishare_records = self.ishares

        for record in ishare_records:
            if record["ishares_exchange_name"] not in self.exchanges:
                continue

            if not len(self.exchanges[record["ishares_exchange_name"]]):
                continue

            task = {
                "ISIN": record["isin"],
                "Name": record["ishares_name"],
                "Ticker": record["exchange_ticker"],
                "Exchange": record["ishares_exchange_name"],
                "exch": self.exchanges[record["ishares_exchange_name"]],
            }

            self.tasks.append(task)
            self.ishares_map[record["isin"] + ":" + record["ishares_exchange_name"]] = (
                record
            )

    def _cleanup_duplicates(self):
        filtered = []
        for item in self.raw_openfigi_resp:
            data = item["data"]
            resp = item["response"]
            conflict = any(
                other["data"] == data and bool(other["response"]) != bool(resp)
                for other in self.raw_openfigi_resp
            )
            if conflict:
                if resp:
                    filtered.append(item)
            else:
                filtered.append(item)
        self.raw_openfigi_resp = filtered

    def _filter_exchange_pairs(self):
        for irow in self.raw_openfigi_resp:
            if not irow["response"]:
                continue

            for zrow in self.raw_openfigi_resp:
                if not zrow["response"]:
                    continue

                if irow["response"]["name"] != zrow["response"]["name"]:
                    continue

                if irow["response"]["exchCode"] == zrow["response"]["exchCode"]:
                    continue

                resolved_exch_code = self.resolve_exch_pair(
                    irow["response"]["exchCode"], zrow["response"]["exchCode"]
                )
                if not resolved_exch_code:
                    continue

                if resolved_exch_code == irow["response"]["exchCode"]:
                    key = zrow
                elif resolved_exch_code == zrow["response"]["exchCode"]:
                    key = irow

                self.raw_openfigi_resp.remove(key)

    def resolve_exch_pair(self, exch_a, exch_b):
        for exchange_dict in self.exchanges.values():
            exchanges_value = exchange_dict.values()
            if exch_a in exchanges_value and exch_b in exchanges_value:
                return exchange_dict[1]

    def _assemble_final(self):
        logger.info("Assembling final results")
        for item in self.raw_openfigi_resp:
            resp = item.get("response", {})
            key = f"{item['data']['ISIN']}:{item['data']['Exchange']}"
            original = self.ishares_map.get(key)
            if not original:
                logger.error(f"Original record not found for {key}")
                continue

            if resp:
                if resp.get("ticker") != resp.get("securityDescription"):
                    resp["ticker"] = resp.get("securityDescription")
                original.update(resp)
                self.result.append(original)
            elif self.keep_unlisted:
                self.result.append(original)

    @staticmethod
    def _create_batch(data, count):
        logger.debug(
            f"Creating batch from data of length {len(data)} with count {count}"
        )
        step = int(round(len(data) / count))
        if not step:
            return [data]

        batches = []
        for i in range(0, len(data), step):
            batches.append(data[i: i + step])

        logger.debug(f"Created {len(batches)} batches")
        return batches

    @staticmethod
    def _create_request_body(batch):
        body = []
        for task in batch:
            for code in task["exch"].values():
                body.append(
                    {
                        "body": {
                            "idType": "ID_ISIN",
                            "idValue": task["ISIN"],
                            "exchCode": code,
                        },
                        "task": task,
                    }
                )

        logger.debug(f"Request body created with {len(body)} entries")
        return body
