"""Package de módulos para tratamento de dados especiais"""

import types
import polars as pl
from . import exhaust_diff_by_cilinder, generator_data


def __check_for_addcol(module: types.ModuleType, sn, collist: list[str]) -> list[str]:
    """Verifica se o SN precisa de colunas adicionais e retorna elas"""
    if sn in module.SN_TO_PARSE:
        add_cols = module.ADD_COLS
        # pylint: disable=expression-not-assigned
        [collist.append(col) for col in add_cols if not col in collist]
        # pylint: enable=expression-not-assigned

    return collist


def additional_cols(collist: list[str], sn: str) -> list[str]:
    """Executa rotinas de adição de colunas"""
    collist = __check_for_addcol(exhaust_diff_by_cilinder, sn, collist)
    collist = __check_for_addcol(generator_data, sn, collist)
    return collist


def __check_for_paser(
    module: types.ModuleType, df: pl.DataFrame, sn: str
) -> pl.DataFrame:
    """Verifica SN em um DataFrame e executa os scripts especiais"""
    if sn in module.SN_TO_PARSE:
        df_filter = df.filter(pl.col("Asset") == sn)
        df_parsed, col_result = module.run(df_filter)
        if col_result:
            df_parsed = df.join(
                df_parsed, on=["Timestamp_str", "Asset"], how="left", coalesce=True
            )
            if col_result + "_right" in df_parsed.columns:
                df_parsed = df_parsed.with_columns(
                    pl.coalesce([col_result, col_result + "_right"]).alias(col_result)
                )
                df_parsed = df_parsed.drop(col_result + "_right")
    else:
        df_parsed = df
    return df_parsed


def run_currentdata(df):
    """Verifica SN em um DataFrame e executa os scripts especiais no período de análise atual"""
    list_sn = df["Asset"].unique().to_list()
    df = df.with_columns(
        pl.col("Timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("Timestamp_str")
    )

    for sn in list_sn:
        df = __check_for_paser(exhaust_diff_by_cilinder, df, sn)

    df = df.drop("Timestamp_str")
    return df


def run_all(df: pl.DataFrame):
    """Verifica SN em um DataFrame e executa os scripts especiais em todo o banco de dados"""

    return df


if __name__ == "__main__":

    print("Execute o script através da GUI!")
