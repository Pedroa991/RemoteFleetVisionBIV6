"""Package do TrendBot"""

import polars as pl
from . import trendbot_func


def run_trendbot(
    df: pl.DataFrame, pathbaseline: str, pathmonthly: str, pathcomments: str
):
    """Principal rotina do Trendbot"""

    trendbot_func.main_trendbot(df, pathbaseline, pathmonthly, pathcomments)


if __name__ == "__main__":

    print("Execute o script através da GUI!")
