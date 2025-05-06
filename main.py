import pandas as pd
from icecream import ic

from get_db_prices import get_db_prices
from site_prices import update_db_site_prices, get_db_site_prices
from compare_prices import compare_prices


def main():
    update_db_site_prices()
    site_prices = get_db_site_prices()
    site_prices.to_excel("site_prices.xlsx", engine="xlsxwriter", index=False)
    db_prices: pd.DataFrame = get_db_prices()
    db_prices.to_excel("db_prices.xlsx", engine="xlsxwriter", index=False)
    compared_prices = compare_prices(site_prices, db_prices)
    compared_prices.to_excel("compared_prices.xlsx", index=False, engine="xlsxwriter")


if __name__ == "__main__":
    main()
