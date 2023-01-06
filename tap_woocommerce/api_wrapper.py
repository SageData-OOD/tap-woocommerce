from singer import utils
import backoff
import requests
import json
import singer.metrics as metrics
import singer
from woocommerce import API

def _giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429

class ApiWrapper:
    def __init__(self, url, consumer_key, consumer_secret):
        self._logger = singer.get_logger()
        self._wcApi = API(url, consumer_key, consumer_secret, timeout=10)

    @utils.backoff((backoff.expo,requests.exceptions.RequestException), _giveup)
    @utils.ratelimit(20, 1)
    def orders(self, params):
        try:
            self._logger.info("API orders start")
            return self._get("orders", "orders", params=params)
        finally:
            self._logger.info("API orders end")

    def reports(self, date_min, date_max):
        params={
            "date_min": date_min,
            "date_max": date_max
        }
        try:
            self._logger.info("API reports start")
            return self._get("reports", "reports/sales", params)
        finally:
            self._logger.info("API reports end")


    def _get(self, stream_id, url, params):
        with metrics.http_request_timer(stream_id) as timer:
            resp = self._wcApi.get(url, params=params)
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
            resp.raise_for_status()
            return resp.json()

