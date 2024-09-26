"""Rotina pra separação de motores entro do mesmo arquivo"""

import os
import polars as pl

SN_TO_EXTRACT = {
    "S2K00384": ["S1M06675", "S1M07112"],
    "S2K00386": ["S1M06678", "S1M06672"],
    "RPM00819": ["D1K01363"],
}

KEYWORDS = {
    "S2K00384": ["Genset PS", "Genset ST"],
    "S2K00386": ["Genset PS", "Genset ST"],
    "RPM00819": ["C4.4"],
}


def __write_csv_utf_16le(df: pl.DataFrame, path: str):
    """Cria um csv com utf-16le"""
    csv_data = df.write_csv(datetime_format="%Y-%m-%d %H:%M:%S")
    with open(path, "w", encoding="utf-16le") as file:
        file.write(csv_data)


def run(sn: str, pathenglog) -> list[str]:
    """Rotina principal"""

    sn_separated = []

    if not sn in SN_TO_EXTRACT:
        sn_separated.append(None)
        return sn_separated

    df_main = pl.read_csv(pathenglog, encoding="utf-16le", infer_schema_length=0)
    pathlogs = os.path.dirname(pathenglog)

    for i, sn_aux in enumerate(SN_TO_EXTRACT[sn]):
        df_aux = df_main.select(
            ["Sample Time"] + [col for col in df_main.columns if KEYWORDS[sn][i] in col]
        )
        df_main = df_main.select(
            [col for col in df_main.columns if not KEYWORDS[sn][i] in col]
        )
        path_aux = pathlogs + "\\" + sn_aux + ".csv"
        sn_separated.append(sn_aux + ".csv")
        __write_csv_utf_16le(df_aux, path_aux)

    __write_csv_utf_16le(df_main, pathenglog)

    return sn_separated
