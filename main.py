import pandas as pd
from icecream import ic

from get_site_prices import get_site_prices
from get_db_prices import get_db_prices



def compare_prices(data: pd.DataFrame) -> pd.DataFrame:
    data.dropna(subset="Наименование шкафа на сайте", inplace=True)
    data["Цены_равны"] = data["Розница Москва Скид"] == data["Sum-База_РРЦ"]
    data = data[~data["Цены_равны"]]
    return data


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
    # site_prices.to_excel("site_prices.xlsx", index=False, engine="xlsxwriter")        # TODO: Remove before deploy
    # site_prices = pd.read_excel("site_prices.xlsx")



    # db_prices: pd.DataFrame = get_db_prices()
    # db_prices.to_excel("db_prices.xlsx", engine="xlsxwriter", index=False)
    # db_prices = pd.read_excel("db_prices.xlsx", engine="xlsxwriter")
    # joined_data = join_prices(site_prices, db_prices)
    # joined_data.to_excel("joined_data.xlsx", engine="xlsxwriter", index=False)
    # compared_prices = compare_prices(joined_data)
    # compared_prices.to_excel("compared_prices.xlsx", index=False, engine="xlsxwriter")