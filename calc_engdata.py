"""Cálculos comuns para dados de motores"""

from datetime import datetime, time, timedelta
import polars as pl
from classes_rfvbi import PathHolder
import special_parse


def exh_diff(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula a diferença entre as bancadas do motor"""

    if not all(col in df.columns for col in ["EXH_L", "EXH_R"]):
        df = df.with_columns(pl.lit(None).alias("EXH_DIFF"))
        return df

    df = df.with_columns((pl.col("EXH_L") - pl.col("EXH_R")).abs().alias("EXH_DIFF"))

    return df


def median_diff_by_day(df: pl.DataFrame, setcol: set[str]) -> pl.DataFrame:
    """Calcula a mediana após fazer um agrupamento por dia"""

    df = df.with_columns(pl.col("Timestamp").dt.date().alias("Date"))
    calcols = set()

    for col in setcol:
        if not col in df.columns:
            df = df.with_columns(pl.lit(None).alias(col + "_DIFF"))
        else:
            calcols.add(col)

    aggdf = df.group_by([pl.col("Date"), pl.col("Asset")]).agg(
        [
            (pl.col(col).max() - pl.col(col).min()).alias(col + "_DIFF")
            for col in calcols
        ]
    )

    aggdf = aggdf.group_by([pl.col("Asset")]).agg(
        [(pl.col(col + "_DIFF").median()).alias(col + "_MEDIAN") for col in calcols]
    )

    return aggdf


def max_by_asset(df: pl.DataFrame, setcol: set[str]) -> pl.DataFrame:
    """Calcula a diferença do máximo e mínimo das colunas"""

    calcols = set()

    for col in setcol:
        if not col in df.columns:
            df = df.with_columns(pl.lit(None).alias(col + "_DIFF"))
        else:
            calcols.add(col)

    aggdf = df.group_by([pl.col("Asset")]).agg(
        [pl.col(col).max().alias(col + "_MAX") for col in calcols]
    )

    return aggdf


def maint_shift(
    asset: str,
    path_shift: str,
    df_maint_plan_filtered: pl.DataFrame,
    smh_by_day: float,
    fuel_by_day: float,
    smh_last: float,
    fuel_last: float,
    day_last: datetime,
    nclycles_smh: int,
    nclycles_fuel: int,
) -> tuple[float | None, float | None]:
    """Retorna os valores de correção de acordo a última manutenção"""
    df_maint_shift = pl.read_excel(path_shift, sheet_name="By SN")
    df_maint_shift = df_maint_shift.filter(pl.col("SN") == asset)

    if df_maint_shift.is_empty():
        return 0, 0

    last_main_name = df_maint_shift.item(0, "Maintenance Name")
    last_main_smh = df_maint_shift.item(0, "Run Hours")
    last_main_fuel = df_maint_shift.item(0, "Total Fuel (L)")
    last_main_date = df_maint_shift.item(0, "Date")

    df_maint_plan_last = df_maint_plan_filtered.filter(
        pl.col("Maintenance Name") == last_main_name
    )
    smh_std_main = df_maint_plan_last.item(0, "Target SMH")
    fuel_std_main = df_maint_plan_last.item(0, "Target Fuel (L)")

    if last_main_date:
        last_main_date = datetime.combine(last_main_date, time())
        daydiff = (day_last - last_main_date).total_seconds() / (24 * 3600)

        if not last_main_smh:
            if smh_by_day:
                last_main_smh = smh_last - daydiff * smh_by_day
            else:
                last_main_smh = None

        if not last_main_fuel:
            if fuel_by_day:
                last_main_fuel = fuel_last - daydiff * fuel_by_day
            else:
                last_main_fuel = None

    try:
        smh_shift = last_main_smh - last_main_smh * (nclycles_smh) - smh_std_main
    except TypeError:
        smh_shift = None

    try:
        fuel_shift = last_main_fuel - last_main_fuel * (nclycles_fuel) - fuel_std_main
    except TypeError:
        fuel_shift = None

    return smh_shift, fuel_shift


def maintenance_est(df: pl.DataFrame, path_holder: PathHolder) -> pl.DataFrame:
    """Estimativa de manutenção"""

    print("\nIniciando cálculo de manutenção...\n")

    df_day = median_diff_by_day(df, {"SMH", "Total_Fuel"})
    df_max = max_by_asset(df, {"SMH", "Total_Fuel", "Timestamp"})
    df_info_maint = df_day.join(df_max, on="Asset", how="left")

    df_asset_info = pl.read_excel(path_holder.asset_info, sheet_name="ASSET_LIST")
    df_maint_plan = pl.read_excel(path_holder.maintanance_plan, sheet_name="By Model")

    df_full_maint_output = pl.DataFrame(
        schema={
            "Asset": pl.Utf8,
            "Maintenance Name": pl.String,
            "Maintenance Type": pl.String,
            "Dias estimados SMH": pl.Datetime,
            "Dias estimados Fuel": pl.Datetime,
            "smh_by_day": pl.Float64,
            "fuel_by_day": pl.Float64,
        }
    )

    for asset in df_asset_info["Serial"].to_list():
        df_info_maint_filtered = df_info_maint.filter(pl.col("Asset") == asset)

        if df_info_maint_filtered.is_empty():
            print(
                f"{asset} Sem informações para cálculo de manutenção! Falta de dados!"
            )
            continue

        df_asset_info_filtered = df_asset_info.filter(pl.col("Serial") == asset)
        asset_model = df_asset_info_filtered.item(0, "Model")
        df_maint_plan_filtered = df_maint_plan.filter(pl.col("Model") == asset_model)

        if df_maint_plan_filtered.is_empty():
            print(
                f"{asset} Sem informações para cálculo de manutenção! Insira o plano de manutanção"
            )
            continue

        smh_final_maint = df_maint_plan_filtered["Target SMH"].max()
        fuel_final_maint = df_maint_plan_filtered["Target Fuel (L)"].max()

        smh_by_day = df_info_maint_filtered.item(0, "SMH_MEDIAN")
        fuel_by_day = df_info_maint_filtered.item(0, "Total_Fuel_MEDIAN")

        smh_last = df_info_maint_filtered.item(0, "SMH_MAX")
        fuel_last = df_info_maint_filtered.item(0, "Total_Fuel_MAX")
        day_last = df_info_maint_filtered.item(0, "Timestamp_MAX")

        if smh_last is None and fuel_last is None:
            continue

        if smh_last is None or smh_by_day is None or smh_by_day == 0:
            nclycles_smh = None
            has_smh = False
        else:
            nclycles_smh = int(smh_last / smh_final_maint)
            has_smh = True

        if fuel_last is None or fuel_by_day is None or fuel_by_day == 0:
            nclycles_fuel = None
            has_fuel = False
        else:
            nclycles_fuel = int(fuel_last / fuel_final_maint)
            has_fuel = True

        smh_shift, fuel_shift = maint_shift(
            asset,
            path_holder.maintanance_shift,
            df_maint_plan_filtered,
            smh_by_day,
            fuel_by_day,
            smh_last,
            fuel_last,
            day_last,
            nclycles_smh,
            nclycles_fuel,
        )

        if has_smh:
            smh_corrected = smh_last - smh_final_maint * nclycles_smh - smh_shift
            df_maint_smh = df_maint_output = df_maint_plan_filtered.select(
                [
                    pl.col("Maintenance Name").alias("Maintenance Name"),
                    (
                        day_last
                        + pl.duration(
                            days=(
                                (
                                    (
                                        smh_corrected
                                        - pl.col("Target SMH")
                                        * (smh_corrected / pl.col("Target SMH")).cast(
                                            int
                                        )
                                    )
                                    - pl.col("Target SMH")
                                ).abs()
                                / smh_by_day
                            )
                        )
                    )
                    .dt.date()
                    .alias("Dias estimados SMH"),
                ]
            )
        else:
            df_maint_smh = df_maint_plan_filtered.select(
                [
                    pl.col("Maintenance Name").alias("Maintenance Name"),
                    pl.lit(None).alias("Dias estimados SMH"),
                ]
            )

        if has_fuel:
            fuel_corrected = fuel_last - fuel_final_maint * nclycles_fuel - fuel_shift
            df_maint_fuel = df_maint_output = df_maint_plan_filtered.select(
                [
                    pl.col("Maintenance Name").alias("Maintenance Name"),
                    (
                        day_last
                        + pl.duration(
                            days=(
                                (
                                    (
                                        fuel_corrected
                                        - pl.col("Target Fuel (L)")
                                        * (
                                            fuel_corrected / pl.col("Target Fuel (L)")
                                        ).cast(int)
                                    )
                                    - pl.col("Target Fuel (L)")
                                ).abs()
                                / fuel_by_day
                            )
                        )
                    )
                    .dt.date()
                    .alias("Dias estimados Fuel"),
                ]
            )
        else:
            df_maint_fuel = df_maint_plan_filtered.select(
                [
                    pl.col("Maintenance Name").alias("Maintenance Name"),
                    pl.lit(None).alias("Dias estimados Fuel"),
                ]
            )

        df_maint_output = df_maint_plan_filtered.select(
            [
                pl.lit(asset).alias("Asset"),
                pl.col("Maintenance Name").alias("Maintenance Name"),
                pl.col("Maintenance Type").alias("Maintenance Type"),
            ]
        )

        df_maint_output = df_maint_output.join(
            df_maint_smh, on="Maintenance Name", how="left"
        )
        df_maint_output = df_maint_output.join(
            df_maint_fuel, on="Maintenance Name", how="left"
        )
        df_maint_output = df_maint_output.with_columns(
            pl.lit(smh_by_day).alias("smh_by_day"),
            pl.lit(fuel_by_day).alias("fuel_by_day"),
        )

        df_full_maint_output = pl.concat(
            [df_full_maint_output, df_maint_output], how="vertical_relaxed"
        )

    valid_start = datetime.now() - timedelta(days=356 * 50)
    valid_end = datetime.now() + timedelta(days=356 * 50)

    df_full_maint_output = df_full_maint_output.with_columns(
        pl.when(
            (pl.col("Dias estimados SMH") < valid_start)
            | (pl.col("Dias estimados SMH") > valid_end)
        )
        .then(None)
        .otherwise(pl.col("Dias estimados SMH"))
        .alias("Dias estimados SMH"),
        pl.when(
            (pl.col("Dias estimados Fuel") < valid_start)
            | (pl.col("Dias estimados Fuel") > valid_end)
        )
        .then(None)
        .otherwise(pl.col("Dias estimados Fuel"))
        .alias("Dias estimados Fuel"),
    )

    print(df_full_maint_output, "\n")
    print("Cálculo de manutenção finalizado!\n")
    df_full_maint_output.write_csv(
        path_holder.maintenance_output, datetime_format="%Y-%m-%d %H:%M:%S"
    )


def run_currentdata(df: pl.DataFrame) -> pl.DataFrame:
    """Executa as rotinas de cálculo para os dados de motor
    Otimizado para os dados analisados no período atual
    """
    df = special_parse.run_currentdata(df)
    df = exh_diff(df)
    return df


def run_alldata(df: pl.DataFrame, path_holder: PathHolder) -> pl.DataFrame:
    """Executa as rotinas de cálculo para os dados de motor
    Otimizado para todo o banco de dados com os dados atualizados
    """
    df = special_parse.run_all(df)
    maintenance_est(df, path_holder)
    return df


if __name__ == "__main__":

    print("Execute o script através da GUI!")
