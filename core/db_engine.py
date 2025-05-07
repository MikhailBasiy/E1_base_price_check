import json
from os import getenv

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def get_conf():
    load_dotenv()
    config_path = getenv("CONFIG_PATH")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_engine(db_name: str) -> Engine:
    config = get_conf()
    DB_HOST = config[db_name]["DB_HOST"]
    DB_NAME = config[db_name]["DB_NAME"]
    DB_USERNAME = config[db_name]["DB_USERNAME"]
    DB_PASSWORD = config[db_name]["DB_PASSWORD"]

    engine = create_engine(
        f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/"
        f"{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return engine
