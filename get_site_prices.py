import pandas as pd
import requests
from requests.exceptions import HTTPError
from http import HTTPStatus
from time import sleep
from dotenv import load_dotenv
from os import getenv

from icecream import ic


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    data.to_excel("raw_site_data.xlsx", index=False, engine="xlsxwriter")
    data[
            ["Высота, мм", 
            "Ширина, мм", 
            "Глубина, мм", 
            "Розница Москва Скид", 
            "Розница Сибирь Скид"]
        ] = \
            data[
                ["Высота, мм", 
                "Ширина, мм", 
                "Глубина, мм", 
                "Розница Москва Скид", 
                "Розница Сибирь Скид"]
            ].fillna(0)
    
    data = data.astype(         ### walkaround for ValueError: invalid literal for int() with base 10
        {"Высота, мм": "float", 
         "Ширина, мм": "float", 
         "Глубина, мм": "float", 
         "Розница Москва Скид": "float", 
         "Розница Сибирь Скид": "float"}
    )
    data = data.astype(
        {"Высота, мм": "int64", 
         "Ширина, мм": "int64", 
         "Глубина, мм": "int64", 
         "Розница Москва Скид": "int64", 
         "Розница Сибирь Скид": "int64"}
    )
    data["Цвет профиля"] = data["Цвет профиля"].str.replace(" профиль", "")
    return data


def normalize_json(data: dict) -> pd.DataFrame:
    rows: list[dict] = []
    for product_id, attributes in data.items():
        row = {'ID': product_id}
        for value in attributes.values():
            row[value['NAME']] = value['VALUE']
        rows.append(row)
    normalized_data = pd.DataFrame(rows)
    return normalized_data


def collect_raw_data():
    load_dotenv()
    API_URL = getenv("API_URL")
    LIMIT = 2000
    MAX_PAGE = 5
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
                url = f"{API_URL}/?page={page_num}&limit={LIMIT}"
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
    site_prices: pd.DataFrame = clean_data(site_prices)
    return site_prices
