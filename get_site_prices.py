import pandas as pd
import requests
from requests.exceptions import HTTPError
from http import HTTPStatus
from time import sleep


def normalize_json(data: dict) -> pd.DataFrame:
    rows: list[dict] = []
    for product_id, attributes in data.items():
        row = {'ID': product_id}
        for value in attributes.values():
            row[value['NAME']] = value['VALUE']
        rows.append(row)
    rows = pd.DataFrame(rows)
    return rows


def collect_raw_data():
    LIMIT = 2000
    MAX_PAGE = 200
    MAX_RETRIES = 3
    RETRY_CODES = [
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    ]
    collected_data = {}
    for page_num in range(1, MAX_PAGE):
        for retry in range(MAX_RETRIES):
            try:
                url = f"{API_URL}" \
                    f"sku_list.get/?page={page_num}&limit={LIMIT}"
                print(url)
                response = requests.get(url)
                response.raise_for_status()
                response = response.json()
            except HTTPError as e:
                print(e)
                if e.response.status_code in RETRY_CODES:
                    print(f"Response status: {e.response.status_code}")
                    print("Repeat")
                    sleep(3)
                    continue
                else:
                    print(f"Response: {e.response.status_code}")
            else:
                products_data = response["result"]["response"]
                if not products_data:
                    continue
                else:
                    collected_data.update(products_data)
                    print(f"Success")
                    break
        if not products_data:
            break
    return collected_data


def get_site_prices() -> pd.DataFrame:
    site_prices: dict[dict] = collect_raw_data()   
    site_prices: pd.DataFrame = normalize_json(site_prices)
    return site_prices
