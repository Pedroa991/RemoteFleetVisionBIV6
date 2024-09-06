"""Módulo calcula diferencial de temperatura entre cilindros"""

import polars as pl

SN_TO_PARSE = ("WPW00989", "WPW00990", "WPW00998", "WPW01003")


def list_col_cil(ncil: int) -> list[str]:
    """Função para criar lista com dos nomes das colunas de temp de cilindro"""
    list_colname_ciltemp = [
        "Engine Exhaust Gas Port " + str(x + 1) + " Temperature [Deg. C]"
        for x in range(ncil)
    ]
    return list_colname_ciltemp


def run(df: pl.DataFrame) -> tuple[pl.DataFrame, str]:
    """Executa rotina"""

    list_colname_ciltemp = list_col_cil(16)
    col_to_parse = [col for col in list_colname_ciltemp if col in df.columns]
    col_result = "Diff_Temp_Cilindro"
    if not col_to_parse:
        return pl.DataFrame(), col_result

    df_parsed = df.select(
        pl.col("Timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("Timestamp_str"),
        pl.col("Asset"),
        (pl.max_horizontal(col_to_parse) - pl.min_horizontal(col_to_parse)).alias(
            col_result
        ),
    )

    return df_parsed, col_result


list_colname_ciltranformer = [
    "Cylinder # " + str(x + 1) + " Transformer Secondary Output Voltage Percentage [%]"
    for x in range(16)
]

list_colname_ciltranformer.extend(
    [
        "Cylinder #"
        + str(x + 1)
        + " Transformer Secondary Output Voltage Percentage [%]"
        for x in range(16)
    ]
)


ADD_COLS = list_col_cil(16)
ADD_COLS.append("Diff_Temp_Cilindro")
ADD_COLS.extend(list_colname_ciltranformer)

if __name__ == "__main__":

    print("Execute o script através da GUI!")
