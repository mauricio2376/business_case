# dimensions/dim_region_loader.py

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader

class DimRegionLoader(BaseLoader):
    """
    Classe responsável pela carga da dimensão de regiões (region).
    Lê os dados da camada de staging, aplica deduplicação e grava no catálogo como Delta Table.
    """

    def load(self):
        """
        Executa o processo de carga da dimensão de regiões, desde a leitura até a escrita no catálogo.
        """
        try:
            self.log("Iniciando carga da dimensão dim_region")

            # Leitura da staging
            df = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_sales_20210726")

            # Seleção e deduplicação
            dim_region = df.select("BTLR_ORG_LVL_C_DESC").dropDuplicates()

            # Padronização para lowercase
            dim_region = dim_region.toDF(*[c.lower() for c in dim_region.columns])

            # Escrita na camada de dimensão
            dim_region.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable("beverage_analytics.dim.dim_region")

            self.log("Carga concluída com sucesso em: beverage_analytics.dim.dim_region")

        except Exception as e:
            self.log(f"Erro durante a carga da dimensão dim_region: {str(e)}")
            raise
