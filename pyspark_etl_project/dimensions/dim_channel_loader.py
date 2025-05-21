# dimensions/dim_channel_loader.py

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader

class DimChannelLoader(BaseLoader):
    """
    Classe responsável pela carga da dimensão de canais (channel).
    Lê os dados da camada de staging, aplica deduplicação e grava no catálogo como Delta Table.
    """

    def load(self):
        """
        Executa o processo de carga da dimensão de canais, desde a leitura até a escrita no catálogo.
        """
        try:
            self.log("Iniciando carga da dimensão dim_channel")

            # Leitura da staging
            df = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_channel_group_20210726")

            # Seleção e deduplicação
            dim_channel = (
                df.select("TRADE_CHNL_DESC", "TRADE_GROUP_DESC", "TRADE_TYPE_DESC")
                .dropDuplicates()
            )

            # Padronização para lowercase
            dim_channel = dim_channel.toDF(*[c.lower() for c in dim_channel.columns])

            # Escrita na camada de dimensão
            dim_channel.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable("beverage_analytics.dim.dim_channel")

            self.log("Carga concluída com sucesso em: beverage_analytics.dim.dim_channel")

        except Exception as e:
            self.log(f"Erro durante a carga da dimensão dim_channel: {str(e)}")
            raise
