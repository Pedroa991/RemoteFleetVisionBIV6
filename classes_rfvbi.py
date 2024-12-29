"""Classes para RFV TO BI"""

import os
import polars as pl

SHAREPOINT_NAME = "PBI_BD - BD_Clientes"


class PathHolder:
    """Objeto para lidar com todos os diretórios"""

    def __init__(self, dbpath: str) -> None:
        self.db = dbpath
        self.asset_info = os.path.dirname(self.db) + "/00 - INFOS/ASSET_INFO.xlsx"
        self.config = os.path.dirname(self.db) + "/00 - INFOS/ConfigScript.xlsx"
        self.englogs = self.db + "/englogs/"
        self.eng_output = self.db + "/history_output.csv"
        self.event_output = self.db + "/events_output.csv"
        self.maintenance_output = self.db + "/maintenance_output.csv"
        self.maintanance_shift = (
            os.path.dirname(self.db) + "/00 - INFOS/MAINTENANCE_SHIFT.xlsx"
        )
        self.maintanance_plan = ""

        self.trendbot = os.path.dirname(self.db) + "/04 - TRENDBOT/"
        self.tb_baseline = self.trendbot + "baseline.csv"
        self.tb_monthly = self.trendbot + "engs_statistics_monthly.csv"
        self.tb_comments = self.trendbot + "comments.csv"

        self._add_commonpaths()

    def _add_commonpaths(self):
        """Adiciona os caminhos do arquivo de configuração como atributos"""
        df_path = pl.read_excel(self.config, sheet_name="CaminhosComuns")
        set_commonpaths = set(zip(df_path["Nome"], df_path["Caminho"]))

        for attname, path in set_commonpaths:

            if os.path.exists(path):
                final_path = path
            else:
                # Handle paths for different users assuming the same folder name
                attib_path = path.split(SHAREPOINT_NAME)[1]
                user_path = self.db.split(SHAREPOINT_NAME)[0]
                final_path = user_path + SHAREPOINT_NAME + attib_path

                if not os.path.exists(final_path):
                    raise ValueError(
                        f"{attname} falhou em ser definido em :\n{final_path}"
                    )

            setattr(self, attname, final_path)


if __name__ == "__main__":

    print("Execute o script através da GUI!")
