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
    return


if __name__ == "__main__":
    # site_prices: pd.DataFrame = get_site_prices()
    # site_prices.to_excel("site_prices.xlsx", index=False, engine="xlsxwriter")        # TODO: Remove before deploy
    db_prices: pd.DataFrame = get_db_prices()
    print(db_prices)
    # joined_data = join_prices(site_prices, db_prices)
    # compared_prices = compare_prices(joined_data)
    # compared_prices = pd.to_excel("compared_prices", index=False, engine="xlsxwriter")