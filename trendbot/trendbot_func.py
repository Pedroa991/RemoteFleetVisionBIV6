import polars as pl

list_parameters = [
    "Batt",
    "Boost",
    "Coolant_Temp",
    "Crank_Press",
    "EXH_DIFF",
    "EXH_L",
    "EXH_R",
    "Fuel_Press",
    "Fuel_Rate",
    "Inlet_Air_Temp",
    "Oil_Press",
    "Oil_Temp",
]

tolerance = 0.10


# Aux funcions
def __categorize_load(load):
    """Categorização de 10 em 10 do fator de carga do motor"""
    lower = (load // 10) * 10
    upper = lower + 10
    return f"{lower}-{upper}"


def __mean_comparison(baseline_col, month_col):
    """Comparação entre números com tolerância"""
    return (
        pl.when(month_col > baseline_col * (1 + tolerance))
        .then(pl.lit("Valor mensal acima do baseline"))
        .when(month_col < baseline_col * (1 - tolerance))
        .then(pl.lit("Valor mensal abaixo do baseline"))
        .otherwise(pl.lit("Valor mensal dentro do baseline"))
    )


# Main functions
def __calculate_baseline(df, column):
    """Calcula estatisticas para baseline"""
    return (
        df.group_by(["Asset", "Load Interval"])
        .agg(
            [
                pl.col(column).mean().alias("Mean"),
                pl.col(column).median().alias("Median"),
                pl.col(column).std().alias("STD Deviation"),
                pl.col(column).count().alias("Count"),
            ]
        )
        .with_columns(pl.lit(column).alias("Parameter"))
    )


def __calculate_monthly(df, column):
    """Calcula estatisticas mensais"""
    return (
        df.group_by(["Asset", "Date", "Load Interval"])
        .agg(
            [
                pl.col(column).mean().alias("Mean"),
                pl.col(column).median().alias("Median"),
                pl.col(column).std().alias("STD Deviation"),
                pl.col(column).count().alias("Count"),
            ]
        )
        .with_columns(pl.lit(column).alias("Parameter"))
    )


def comments_generator(df_baseline, df_monthly):
    """Gera comentários automaticamente"""
    df_cmts_month = df_monthly.group_by(["Date", "Asset", "Parameter"]).agg(
        [
            (pl.col("Mean") * pl.col("Count")).sum().alias("Weighted_Mean_Sum"),
            pl.col("Count").sum().alias("Count_Sum"),
        ]
    )

    df_cmts_month = df_cmts_month.with_columns(
        (pl.col("Weighted_Mean_Sum") / pl.col("Count_Sum")).alias(
            "Weighted_Mean_Monthly"
        )
    )

    df_cmts_baseline = df_baseline.group_by(["Asset", "Parameter"]).agg(
        [
            (pl.col("Mean") * pl.col("Count")).sum().alias("Weighted_Mean_Sum"),
            pl.col("Count").sum().alias("Count_Sum"),
        ]
    )

    df_cmts_baseline = df_cmts_baseline.with_columns(
        (pl.col("Weighted_Mean_Sum") / pl.col("Count_Sum")).alias(
            "Weighted_Mean_Baseline"
        )
    )

    df_cmts_month = df_cmts_month.join(
        df_cmts_baseline, on=["Asset", "Parameter"], how="left"
    )

    df_cmts_month = df_cmts_month.select(
        pl.col("Date"),
        pl.col("Asset"),
        pl.col("Parameter"),
        pl.col("Weighted_Mean_Baseline"),
        pl.col("Weighted_Mean_Monthly"),
        __mean_comparison(
            pl.col("Weighted_Mean_Baseline"), pl.col("Weighted_Mean_Monthly")
        ).alias("Status"),
    )

    return df_cmts_month


def main_trendbot(
    df: pl.DataFrame, pathbaseline: str, pathmonthly: str, pathcomments: str
):
    """Principal rotina de calculo do Trendbot"""

    print("\nIniciando TrendBot...\n")

    df = df.with_columns(pl.col("Timestamp").dt.truncate("1mo").alias("Date"))

    df = df.with_columns(
        pl.col("Load")
        .map_elements(__categorize_load, return_dtype=pl.String)
        .alias("Load Interval")
    )

    df_baseline = pl.DataFrame()
    df_monthly = pl.DataFrame()

    for parameter in list_parameters:
        baseline = __calculate_baseline(df, parameter)
        monthly = __calculate_monthly(df, parameter)
        df_baseline = pl.concat([df_baseline, baseline])
        df_monthly = pl.concat([df_monthly, monthly])

    df_comments = comments_generator(df_baseline, df_monthly)

    df_baseline = df_baseline.sort(["Asset", "Parameter", "Load Interval"])
    df_monthly = df_monthly.sort(["Asset", "Parameter", "Date", "Load Interval"])
    df_comments = df_comments.sort(["Asset", "Parameter", "Date"])

    df_baseline.write_csv(pathbaseline, datetime_format="%Y-%m-%d %H:%M:%S")
    df_monthly.write_csv(pathmonthly, datetime_format="%Y-%m-%d %H:%M:%S")
    df_comments.write_csv(pathcomments, datetime_format="%Y-%m-%d %H:%M:%S")

    print("\nCálculos do TrendBot finalizados!\n")


if __name__ == "__main__":

    print("Execute o script através da GUI!")
