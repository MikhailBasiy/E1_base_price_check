import pandas as pd


def join_prices(site_prices: pd.DataFrame, db_prices: pd.DataFrame) -> pd.DataFrame:
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
            "Компоновка корпуса",
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
        suffixes=("_сайт", "_БД"),
    )
    return joined_prices


def compare_prices(site_prices: pd.DataFrame, db_prices: pd.DataFrame) -> pd.DataFrame:
    data = join_prices(site_prices, db_prices)
    data.dropna(subset="Наименование шкафа на сайте", inplace=True)
    data["Цены_равны"] = data["Розница Москва Скид"] == data["Sum-База_РРЦ"]
    data = data[~data["Цены_равны"]]
    return data
