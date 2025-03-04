"""
RFV to BI
Developed by Pedro Venancio
"""

import os
from shutil import rmtree
from functools import reduce
from datetime import timedelta
import zipfile
import re
import polars as pl
import fastexcel  # pylint: disable=unused-import
from classes_rfvbi import PathHolder
import calc_engdata
from special_parse import additional_cols, eng_separator
from trendbot import run_trendbot


SCRIPT_VERSION = "V6.4.1"

ESSENTIALS_COL = (
    "Timestamp",
    "Load",
    "RPM",
    "Coolant_Temp",
    "Oil_Press",
    "Fuel_Rate",
    "EXH_L",
    "EXH_R",
    "Total_Fuel",
    "SMH",
)

DICT_COLNAME = {
    "Timestamp": ["Sample Time"],
    "Load": ["Engine Load Factor [%]", "Engine Percent Load At Current Speed [%]"],
    "RPM": ["Engine Speed [RPM]"],
    "Coolant_Temp": ["Engine Coolant Temperature [Deg. C]"],
    "Oil_Press": ["Engine Oil Pressure [kPa]", "Engine Oil Pressure 1 [kPa]"],
    "Oil_Temp": ["Engine Oil Temperature [Deg. C]"],
    "Batt": ["Battery Voltage [volts]", "Battery Potential / Power Input 1 [volts]"],
    "Boost": ["Boost Pressure [kPa]", "Engine Intake Manifold #1 Pressure [kPa]"],
    "Fuel_Rate": [
        "Fuel Consumption Rate [L/hr]",
        "Engine Fuel Rate [L/hr]",
        "Fuel Consumption Rate",
    ],
    "EXH_L": [
        "Left Exhaust Temperature [Deg. C]",
        "Engine Exhaust Manifold Bank 1 Temperature 1 [Deg. C]",
    ],
    "EXH_R": [
        "Right Exhaust Temperature [Deg. C]",
        "Engine Exhaust Manifold Bank 2 Temperature 1 [Deg. C]",
    ],
    "Total_Fuel": ["Total Fuel [L]", "Engine Total Fuel Used [L]"],
    "SMH": [
        "Run Hours [Hrs]",
        "Run Hours [Hours]",
        "Engine Run Hours [Hrs]",
        "Engine Run Hours [Hours]",
        "Total Time [Hours]",
        "Total Operating Hours [Hours]",
        "Engine Total Hours of Operation [Hours]",
        "Engine Total Hours of Operation [Hrs]",
        "Total Operating Hours [Hrs]",
        "Total Time [Hrs]",
        "PLE Run Hours [Hours]",
        "PLE Run Hours [Hrs]",
    ],
    "Fuel_Press": ["Fuel Pressure [kPa]", "Engine Fuel Delivery Pressure [kPa]"],
    "Crank_Press": ["Crankcase Pressure [kPa]"],
    "Aftercooler_Temp": [
        "Engine Intercooler Temperature [Deg. C]",
        "Aftercooler Temperature [Deg. C]",
    ],
    "Inlet_Air_Temp": ["Inlet Air Temperature [Deg. C]"],
    "Latitude": ["Latitude [Degrees]"],
    "Longitude": ["Longitude [Degrees]"],
    "Vessel_Speed": ["Speed [km/h]"],
    "Heading": ["Heading [Degrees]"],
}

TUPLE_COLEVENT = (
    "Timestamp",
    "Type",
    "Source",
    "Code",
    "Severity",
    "Description",
    "Asset",
)

# Auxiliar Funtions


def delete_data(dbpath: str) -> None:
    """Deleta todos os arquivos e pasta de uma pasta"""
    tx_warning = """Opção sem concatenação selecionada!
Deletando dados anteriores..."""
    print(tx_warning)
    for filename in os.listdir(dbpath):
        file_path = os.path.join(dbpath, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            rmtree(file_path)


def prep_englog(englogpath: str, dest: str) -> None:
    """Extrai o zip dos motores para uma pasta temporária
    e retorna uma lista com os nomes dos arquivos"""
    with zipfile.ZipFile(englogpath, "r") as zipengs:
        zipengs.extractall(dest)
        list_englogs = list(zipengs.namelist())
    return list_englogs


def get_sn(nome_arquivo: str) -> str | None:
    """Extrai o Serial Number do nome dos arquivos"""
    match = re.search(r"([A-Z0-9]{8})\.csv$", nome_arquivo, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return None


def get_assets(path):
    """Retorna um set com todos os ativos cadastrados"""
    df_assetinfo = pl.read_excel(path, sheet_name="ASSET_LIST")
    set_assets = set(df_assetinfo["Serial"].to_list())
    return set_assets


def numeric_convert(col):
    """Tenta converter uma coluna para float"""
    type(col)
    try:
        return col.cast(pl.Float64)
    except pl.exceptions.InvalidOperationError:
        return col


def get_database_data(path: str, list_colstd: list[str] | tuple[str]) -> pl.DataFrame:
    """Abre tabela de dados e se não existir cria uma vazia"""
    if os.path.isfile(path):
        df = pl.read_csv(path, infer_schema_length=0)
        df = define_types(df, list_colstd)
    else:
        df = pl.DataFrame({col: [] for col in list_colstd})
    return df


def concatenate_dfs(main_df: pl.DataFrame, aux_df: pl.DataFrame) -> pl.DataFrame:
    """Concatena dataframes assegurando dtypes compatíveis"""
    all_columns = set(main_df.columns).union(set(aux_df.columns))
    main_df = main_df.select(
        pl.col(col) if col in main_df.columns else pl.lit(None).alias(col)
        for col in all_columns
    )
    aux_df = aux_df.select(
        pl.col(col) if col in aux_df.columns else pl.lit(None).alias(col)
        for col in all_columns
    )
    try:
        main_df = pl.concat([main_df, aux_df], how="vertical_relaxed")
    except pl.exceptions.SchemaError:
        main_df = main_df.select(
            [pl.col(col).cast(dtype) for col, dtype in aux_df.schema.items()]
        )
        main_df = pl.concat([main_df, aux_df], how="vertical_relaxed")
    return main_df


def datalimiter(df: pl.DataFrame, daylimit: int) -> pl.DataFrame:
    """Limita o banco de dados anterior com base no tempo"""
    if not df.is_empty():
        firstdate = df["Timestamp"].max() - timedelta(days=daylimit)
        df = df.filter(pl.col("Timestamp") >= firstdate)
    return df


# Main Funtions


def rename_col(df: pl.DataFrame, sn: str, path_config: str) -> pl.DataFrame:
    """Renomeia as colunas para padronizar"""
    df_rename = pl.read_excel(path_config, sheet_name="ListaParm")
    df_rename = df_rename.filter(pl.col("SN") == sn)
    dict_rename = dict()
    list_missingcol = []

    if not df_rename.is_empty():
        dict_rename = dict(zip(df_rename["Nome da coluna"], df_rename["Renomear para"]))

    for col_newname in list(DICT_COLNAME.keys()):

        col_found = False

        for keycolname, valuecolname in dict_rename.items():
            if col_newname == valuecolname:
                if keycolname in df.columns:
                    col_found = True
                    break

        if col_found:
            continue

        for col_oldname in DICT_COLNAME[col_newname]:
            if col_oldname in df.columns:
                dict_rename[col_oldname] = col_newname
                col_found = True
                break

        if not col_found and col_newname in ESSENTIALS_COL:
            list_missingcol.append(col_newname)

    if list_missingcol:
        print(
            f"{list_missingcol} Não encontrado(s) para o ativo {sn}! Verifique o ConfigScript!"
        )

    df = df.rename(dict_rename)

    df = df.with_columns([pl.lit(None).alias(colmiss) for colmiss in list_missingcol])

    print("Colunas padronizadas!")

    return df


def define_types(df: pl.DataFrame, list_colstd: list[str]) -> pl.DataFrame:
    """Define tipos de dados das colunas"""
    col_selected = [col for col in list_colstd if col in df.columns]
    df = df.select(col_selected)

    formatlist = [
        "%Y-%m-%d %H:%M:%S",  # 2021-07-15 12:34:56
        "%m/%d/%y %H:%M:%S",  # 7/5/21 12:34:56
        "%m/%d/%y %I:%M %p",  # 7/15/21 12:34 PM
        "%m/%d/%Y %H:%M:%S",  # 07/15/2021 12:34:56
        "%m/%d/%Y %I:%M:%S %p",  # 07/15/2021 12:34:56 PM
        "%m/%d/%y %H:%M",  # 7/5/21 12:34
        "%-m/%d/%Y %-H:%M",  # 6/20/2024 0:00
    ]

    df = df.with_columns(
        pl.coalesce(
            [
                pl.col("Timestamp").str.strptime(pl.Datetime, format, strict=False)
                for format in formatlist
            ]
        ).alias("Timestamp")
    )

    df = df.with_columns(
        [numeric_convert(df[col]) for col in df.columns if col != "Timestamp"]
    )

    return df


def cleandata(df: pl.DataFrame, pathconfig: str, sheetname: str) -> pl.DataFrame:
    """Limpa os dados inválidos e dados não utilizados"""

    df_invalid_data = pl.read_excel(pathconfig, sheet_name=sheetname)
    colname = df_invalid_data.columns[0]
    list_invalid = df_invalid_data[colname].to_list()

    invalid_str = []
    invalid_int = []
    invalid_float = []

    for item in list_invalid:
        if isinstance(item, str):
            invalid_str.append(item)
        elif isinstance(item, int):
            invalid_int.append(item)
            invalid_float.append(float(item))
        elif isinstance(item, float):
            invalid_float.append(item)
            invalid_int.append(int(item))

    df = df.with_columns(
        [
            pl.when(pl.col(col).is_in(invalid_str))
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col in df.columns
            if df[col].dtype == pl.String
        ]
    )

    df = df.with_columns(
        [
            pl.when(pl.col(col).is_in(invalid_float))
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col in df.columns
            if df[col].dtype == pl.Float64
        ]
    )

    df = df.with_columns(
        [
            pl.when(pl.col(col).is_in(invalid_int))
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
            for col in df.columns
            if df[col].dtype == pl.Int64
        ]
    )

    filtermask = reduce(
        lambda a, b: a | b,
        [pl.col(col).is_not_null() for col in df.columns if col != "Timestamp"],
    )

    df = df.filter(filtermask)

    return df


def create_engdata_output(
    set_assets: set[str], path_holder: PathHolder, englogpath: str, is_trendbot: int
) -> None:
    """Rotina para manipulação dos dados dos motores"""

    print("\nIniciando tratamento de dados de motores...\n")

    list_englogs = prep_englog(englogpath, path_holder.englogs)

    list_colstd = list(DICT_COLNAME.keys())
    list_colstd.extend(["Asset"])
    df_full_engs = get_database_data(path_holder.eng_output, list_colstd)
    df_full_engs = datalimiter(df_full_engs, daylimit=6 * 30)
    df_all_current = pl.DataFrame({colname: [] for colname in list_colstd})

    list_sn_add = []
    for pathenglog in list_englogs:
        sn_add = eng_separator.run(get_sn(pathenglog), path_holder.englogs + pathenglog)
        list_sn_add.extend(sn_add)
    list_sn_add = list(filter(lambda item: item is not None, list_sn_add))

    if sn_add:
        print(f"Ativos separados: {list_sn_add}")
        list_englogs.extend(list_sn_add)

    for engfile in list_englogs:

        sn_file = get_sn(engfile)
        path_engfile = path_holder.englogs + engfile

        if not sn_file in set_assets:
            print("\n", sn_file, " Não tem informações em ASSET_INFO.")
            continue

        print(f"\nAtivo: {sn_file}\n")
        df_asset = pl.read_csv(path_engfile, encoding="utf-16le", infer_schema_length=0)
        df_asset = rename_col(df_asset, sn_file, path_holder.config)
        list_colstd = additional_cols(list_colstd, sn_file)
        df_asset = define_types(df_asset, list_colstd)
        df_asset = cleandata(df_asset, path_holder.config, "DadosInvalidos")
        print("Dados limpos!")
        df_asset = df_asset.with_columns(pl.lit(sn_file).alias("Asset"))

        df_all_current = concatenate_dfs(df_all_current, df_asset)

    if df_all_current.is_empty():
        print("\nSem dados de motores!\n")
        df_full_engs.write_csv(
            path_holder.eng_output, datetime_format="%Y-%m-%d %H:%M:%S"
        )
        rmtree(path_holder.englogs)
        return

    df_all_current = calc_engdata.run_currentdata(df_all_current)
    df_full_engs = concatenate_dfs(df_full_engs, df_all_current)
    df_full_engs = calc_engdata.run_alldata(df_full_engs, path_holder)

    print("Cálculos realizados!\n")

    df_full_engs = df_full_engs.with_columns(
        pl.col("Timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("Timestamp_str")
    )
    df_full_engs = df_full_engs.unique(subset=["Asset", "Timestamp_str"], keep="last")
    df_full_engs = df_full_engs.drop("Timestamp_str")

    df_full_engs = df_full_engs.select(
        ["Timestamp", "Asset"]
        + sorted(
            [col for col in df_full_engs.columns if col not in ["Timestamp", "Asset"]]
        )
    )

    df_full_engs = df_full_engs.sort(["Asset", "Timestamp"])
    df_full_engs.write_csv(path_holder.eng_output, datetime_format="%Y-%m-%d %H:%M:%S")
    rmtree(path_holder.englogs)

    print("Dados de motores tratados com sucesso!\n")

    if is_trendbot:
        run_trendbot(
            df_full_engs,
            path_holder.tb_baseline,
            path_holder.tb_monthly,
            path_holder.tb_comments,
        )


def create_events_output(
    set_assets: set[str], path_holder: PathHolder, eventslogpath: str
) -> None:
    """Rotina de tratamento de dados de eventos"""

    print("Iniciando tratamento de dados de eventos...\n")

    df_eventsumraw = pl.read_excel(eventslogpath, sheet_name="Engine Event Summary")

    df_full_events = get_database_data(path_holder.event_output, TUPLE_COLEVENT)

    if df_eventsumraw.is_empty():
        print("\nNão há dados de eventos!\n")
        df_full_events.write_csv(
            path_holder.event_output, datetime_format="%Y-%m-%d %H:%M:%S"
        )
        return

    df_eventsumraw = df_eventsumraw.select(
        pl.col("Unit Name"),
        pl.sum_horizontal(
            "High Severity Count", "Medium Severity Count", "Low Severity Count"
        ).alias("Alert Count"),
    )
    df_eventsumraw = df_eventsumraw.filter(
        (pl.col("Alert Count") > 0) & (pl.col("Unit Name") != "Totals")
    )
    print(df_eventsumraw, "\n")

    list_events_sheetnames = df_eventsumraw["Unit Name"].to_list()

    for evsheetname in list_events_sheetnames:
        sn = evsheetname[-8:]

        if not sn in set_assets:
            print(f"Eventos de {sn} não analisados. Não há informações em ASSET_INFO.")
            continue

        df_asset_events = pl.read_excel(eventslogpath, sheet_name=evsheetname)
        df_asset_events = df_asset_events.rename({"Sample Time": "Timestamp"})
        df_asset_events = cleandata(
            df_asset_events, path_holder.config, "AlertasDelete"
        )
        df_asset_events = df_asset_events.drop_nulls(subset="Code")
        df_asset_events = df_asset_events.with_columns(pl.lit(sn).alias("Asset"))
        df_asset_events = df_asset_events.select(TUPLE_COLEVENT)

        df_full_events = concatenate_dfs(df_full_events, df_asset_events)

    if df_full_events.is_empty():
        print("\nSem dados de eventos!\n")
        df_full_events.write_csv(
            path_holder.event_output, datetime_format="%Y-%m-%d %H:%M:%S"
        )
        return

    df_full_events = df_full_events.with_columns(
        pl.col("Timestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("Timestamp_str")
    )
    df_full_events = df_full_events.unique(
        subset=[
            "Type",
            "Code",
            "Description",
            "Asset",
            "Source",
            "Severity",
            "Timestamp_str",
        ],
        keep="last",
    )
    df_full_events = df_full_events.drop("Timestamp_str")

    df_full_events = df_full_events.select(
        ["Timestamp"]
        + sorted([col for col in df_full_events.columns if col != "Timestamp"])
    )

    df_full_events = df_full_events.sort(["Asset", "Timestamp"])
    df_full_events.write_csv(
        path_holder.event_output, datetime_format="%Y-%m-%d %H:%M:%S"
    )

    print("Eventos tratados com sucesso!\n")


def main(
    dbpath: str, englogpath: str, eventslogpath: str, concatenar: int, is_trendbot: int
) -> None:
    """Função principal RFV TO BI"""

    if not concatenar:
        delete_data(dbpath)

    path_holder = PathHolder(dbpath)
    set_assets = get_assets(path_holder.asset_info)

    if not os.path.isdir(path_holder.trendbot):
        os.makedirs(path_holder.trendbot)

    create_engdata_output(set_assets, path_holder, englogpath, is_trendbot)

    create_events_output(set_assets, path_holder, eventslogpath)


if __name__ == "__main__":

    print("Execute o script através da GUI!")

    # Test Mode

    print("MODO DE TESTE!!!")

    db_path = os.getenv("PATH_BD")
    englog_path = os.getenv("PATH_ENG")
    eventslog_path = os.getenv("PATH_EVENT")
    main(db_path, englog_path, eventslog_path, 1, 0)
