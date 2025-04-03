from dotenv import load_dotenv
from os import getenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    data['Наименование шкафа на сайте'] = data['Наименование шкафа на сайте'] \
        .str.replace('2-дверный', '2-х дверный')
    ### Prime
    data.loc[
        (   (data['Серия'] == 'Прайм') & \
            (data['Тип_шкафа'] == '2-х дверный купе')
        ),
        'Компановка корпуса'] = "Прайм 2х"
    data.loc[
        (   (data['Серия'] == 'Прайм') & \
            (data['Тип_шкафа'] == '3-х дверный купе')
        ),
        'Компановка корпуса'] = "Прайм 3х"
    ### Ekspress
    data.loc[
        (   (data['Серия'] == 'Экспресс') & \
            (data['Глубина'] == 600) & \
            (data['Тип_шкафа'] == '2-х дверный купе')
        ),
        'Компановка корпуса'] = "Экспресс 60 2х"
    data.loc[
        (
            (data['Серия'] == 'Экспресс') & \
            (data['Глубина'] == 600) & \
            (data['Тип_шкафа'] == '3-х дверный купе')
        ),
        'Компановка корпуса'] = "Экспресс 60 3х"
    data.loc[
        (
            (data['Серия'] == 'Экспресс') & \
            (data['Глубина'] == 440) & \
            (data['Тип_шкафа'] == '2-х дверный купе')
        ),
        'Компановка корпуса'] = "Экспресс 45 2х"
    data.loc[
        (
            (data['Серия'] == 'Экспресс') & \
            (data['Глубина'] == 440) & \
            (data['Тип_шкафа'] == '3-х дверный купе')
        ),
        'Компановка корпуса'] = "Экспресс 45 3х"
    ### Locker
    data['Компановка корпуса'] = data['Компановка корпуса'] \
        .str.replace("Локер (без полок, 1 выдвижной модуль)", "Локер (модуль)") \
        .str.replace("Локер (без полок)", "Локер (базовая компоновка)") \
        .str.replace("Локер (С доп. полками, 1 выдвижной модуль)", "Локер (полки, модуль)") \
        .str.replace("Локер (С доп. полками)", "Локер (полки)") \
        .str.replace("Локер (С доп. полками, 2 выдвижных модуля)", "Локер (полки, модуль х2)") \
        .str.replace("Локер полки и штанга", "Локер 120 (базовая компоновка)")
    return data


def get_engine() -> Engine:
    load_dotenv()
    DB_HOST = getenv("DB_HOST")
    DB_NAME = getenv("DB_NAME")
    DB_USERNAME = getenv("DB_USERNAME")
    DB_PASSWORD = getenv("DB_PASSWORD")

    engine = create_engine(
        f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/" \
        f"{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return engine


def get_db_prices() -> pd.DataFrame:
    query = \
        "SELECT [Серия], [Тип_шкафа], [Вариант исполнения шкафа], [Ширина], " \
        "[Высота], [Глубина], [Наименование шкафа на сайте], " \
        "[Компановка корпуса], [Цвет профиля], " \
        "[Sum-База_РРЦ], [Sum-Сибирь_РРЦ] " \
        "FROM [Результат_Стоимость_шкафов_CSKU]"
    engine = get_engine()
    with engine.connect() as con:
        return clean_data(pd.read_sql(query, con))
    

    
