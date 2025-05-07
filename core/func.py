import pandas as pd
from icecream import ic

from core.compare_prices import compare_prices
from core.get_db_prices import get_db_prices
from core.site_prices import get_db_site_prices, update_db_site_prices


def check_base_prices():
    update_db_site_prices()
    site_prices = get_db_site_prices()
    # site_prices.to_excel("site_prices.xlsx", engine="xlsxwriter", index=False)
    db_prices: pd.DataFrame = get_db_prices()
    # db_prices.to_excel("db_prices.xlsx", engine="xlsxwriter", index=False)
    compared_prices = compare_prices(site_prices, db_prices)
    # compared_prices.to_excel("compared_prices.xlsx", index=False, engine="xlsxwriter")
    return compared_prices


def update_base_prices_in_db() -> int:
    return update_db_site_prices()
