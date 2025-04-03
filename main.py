import pandas as pd
from icecream import ic

from get_site_prices import get_site_prices
from get_db_prices import get_db_prices


def compare_prices(joined_data: pd.DataFrame) -> pd.DataFrame:
    return


def join_prices(
        site_prices: pd.DataFrame, 
        db_prices: pd.DataFrame
) -> pd.DataFrame:
    joined_prices = site_prices.merge(
        db_prices, 
        how="left", 
        left_on=[
            "Название карточки",
            "Ширина, мм",
            "Высота, мм",
            "Глубина, мм",
            "Цвет корпуса",
            "Цвет профиля",
            "Компоновка корпуса"
        ],
        right_on=[
            "Наименование шкафа на сайте",
            "Ширина",
            "Высота",
            "Глубина",
            "Вариант исполнения шкафа",
            "Цвет профиля",
            "Компановка корпуса",
        ],
        suffixes=('_сайт', '_БД')
    )
    return joined_prices


if __name__ == "__main__":
    site_prices: pd.DataFrame = get_site_prices()
    site_prices.to_excel("site_prices.xlsx", index=False, engine="xlsxwriter")        # TODO: Remove before deploy
    # site_prices = pd.read_excel("site_prices.xlsx")
    db_prices: pd.DataFrame = get_db_prices()
    db_prices.to_excel("db_prices.xlsx", engine="xlsxwriter", index=False)
    joined_data = join_prices(site_prices, db_prices)
    joined_data.to_excel("joined_data.xlsx", engine="xlsxwriter", index=False)
    # compared_prices = compare_prices(joined_data)
    # compared_prices = pd.to_excel("compared_prices", index=False, engine="xlsxwriter")