import logging
import time

import requests

from config import API_URL

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, api_url: str = API_URL, retries: int = 3):
        self.__api_url = api_url
        self.__retries = retries
        self.__retry_no = 0
        self.__session = requests.session()

    def fetch(self, url: str) -> dict:
        for _ in range(self.__retries - self.__retry_no):
            response = self.__session.get(f"{self.__api_url}/{url}", timeout=10)
            
            if response.status_code > 299:
                self.__retry_no += 1
                logger.error(
                    f"Failed for the {self.__retry_no} time to fetch data from {url}. Sleeping for 10 seconds before retry. " +
                    f"Status code {response.status_code}. Response body: {response.text}. "
                )
                time.sleep(10)
            else:
                return response.json()

        raise Exception(f"Failed to fetch data from {url} within {self.__retries} retries.")
