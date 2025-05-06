import pandas as pd
from sqlalchemy import NVARCHAR, DECIMAL, Integer, text

from db_engine import get_engine
from decimal import Decimal
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
    engine = get_engine()
    try:
        with engine.begin() as con:
            con.execute(text("DELETE FROM Результат_Стоимость_шкафов_Сайт_по_API"))
            data.to_sql(
                name="Результат_Стоимость_шкафов_Сайт_по_API",
                schema="dbo",
                con=con,
                if_exists="append",
                index=False,
                dtype=column_types,
                chunksize=500,
                method=None
            )
    except Exception as e:
        print(e)
        raise
    logger.debug(f"Writing to db finished. Successfully loaded {len(data)} rows.")
    return
