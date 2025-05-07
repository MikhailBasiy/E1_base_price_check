from decimal import Decimal
from http import HTTPStatus
from os import getenv
from time import sleep

import pandas as pd
import requests
from dotenv import load_dotenv
from icecream import ic
from requests.exceptions import HTTPError
from sqlalchemy import DECIMAL, NVARCHAR, Integer, text

from db_engine import get_engine
from logging_settings import get_logger

logger = get_logger(__name__)


def cast_data_types(data: pd.DataFrame) -> pd.DataFrame:
    int_cols = ["ID", "Высота, мм", "Ширина, мм", "Глубина, мм"]
    decimal_cols = ["Розница Москва Скид", "Розница Сибирь Скид"]
    for col in int_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce").astype("Int64")
    for col in decimal_cols:
        data[col] = data[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)
    return data


def write_to_db(data: pd.DataFrame) -> None:
    data: pd.DataFrame = cast_data_types(data)
    column_types = {
        "ID": Integer(),
        "Название": NVARCHAR(255),
        "Высота, мм": Integer(),
        "Ширина, мм": Integer(),
        "Глубина, мм": Integer(),
        "Цвет корпуса": NVARCHAR(255),
        "Цвет профиля": NVARCHAR(255),
        "Компоновка корпуса": NVARCHAR(255),
        "Розница Москва Скид": DECIMAL(10, 2),
        "Розница Сибирь Скид": DECIMAL(10, 2),
        "Внешний код ТП": NVARCHAR(255),
        "Название карточки": NVARCHAR(255),
        "Серия": NVARCHAR(255),
        "Внешний код карточки": NVARCHAR(255),
        "admin_url": NVARCHAR(255),
    }
    logger.debug(f"start writing to db")
    engine = get_engine("E-COM")
    try:
        with engine.connect() as con:
            while True:
                with con.begin():
                    result = con.execute(
                        text(
                            "DELETE TOP(1000) FROM Результат_Стоимость_шкафов_Сайт_по_API"
                        )
                    )
                    if result.rowcount == 0:
                        break

        data.to_sql(
            name="Результат_Стоимость_шкафов_Сайт_по_API",
            schema="dbo",
            con=engine,
            if_exists="append",
            index=False,
            dtype=column_types,
            chunksize=500,
            method=None,
        )
    except Exception as e:
        print(e)
        raise
    logger.debug(f"Writing to db finished. Successfully loaded {len(data)} rows.")
    return


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    data[
        [
            "Высота, мм",
            "Ширина, мм",
            "Глубина, мм",
            "Розница Москва Скид",
            "Розница Сибирь Скид",
        ]
    ] = data[
        [
            "Высота, мм",
            "Ширина, мм",
            "Глубина, мм",
            "Розница Москва Скид",
            "Розница Сибирь Скид",
        ]
    ].fillna(
        0
    )

    data = data.astype(  ### walkaround for ValueError: invalid literal for int() with base 10
        {
            "Высота, мм": "float",
            "Ширина, мм": "float",
            "Глубина, мм": "float",
            "Розница Москва Скид": "float",
            "Розница Сибирь Скид": "float",
        }
    )
    data = data.astype(
        {
            "Высота, мм": "int64",
            "Ширина, мм": "int64",
            "Глубина, мм": "int64",
            "Розница Москва Скид": "int64",
            "Розница Сибирь Скид": "int64",
        }
    )
    data["Цвет профиля"] = data["Цвет профиля"].str.replace(" профиль", "")
    return data


def normalize_json(data: dict) -> pd.DataFrame:
    logger.debug(f"start normalizing data")
    rows: list[dict] = []
    for product_id, attributes in data.items():
        row = {"ID": product_id}
        for value in attributes.values():
            row[value["NAME"]] = value["VALUE"]
        rows.append(row)
    normalized_data = pd.DataFrame(rows)
    logger.debug(f"normalized data len is: {len(normalized_data)}")
    return normalized_data


def collect_raw_data():
    load_dotenv()
    API_URL = getenv("API_URL")
    LIMIT = 2000
    MAX_PAGE = 100
    MAX_RETRIES = 3
    RETRY_CODES = [
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    ]
    collected_data = {}
    logger.debug(f"data is being collected...")
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
    logger.debug(f"data successfully collected using API")
    return collected_data


def update_db_site_prices() -> None:
    site_prices: dict[dict] = collect_raw_data()
    site_prices: pd.DataFrame = normalize_json(site_prices)
    site_prices: pd.DataFrame = clean_data(site_prices)
    write_to_db(site_prices)
    return


def get_db_site_prices() -> pd.DataFrame:
    engine = get_engine("E-COM")
    with engine.connect() as con:
        return pd.read_sql("Результат_Стоимость_шкафов_Сайт_по_API", con=con)
