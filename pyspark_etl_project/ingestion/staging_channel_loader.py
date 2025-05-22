# staging_channel_loader.py
# ===============================================
# Finalidade: Carregar dados de canais para a tabela de staging
# ===============================================

from ingestion.base_loader import BaseLoader
from pyspark.sql import DataFrame

class StagingChannelLoader(BaseLoader):
    """
    Loader para ingestão da base de canais (channel group) para a camada staging.
    """

    def __init__(self):
        """
        Inicializa o loader com os parâmetros específicos de path e destino.
        """
        super().__init__()
        self.catalog = "beverage_analytics"
        self.schema = "staging"
        self.table = "abi_bus_case1_beverage_channel_group_20210726"
        self.path = "/FileStore/bronze/abi_bus_case1_beverage_channel_group_20210726.csv"

    def read(self) -> DataFrame:
        """
        Lê o arquivo CSV de canais com as opções de encoding e separador definidos.

        Returns:
            DataFrame: Dados lidos do arquivo CSV.
        """
        try:
            self.log(f"Lendo arquivo de canais de: {self.path}")
            return self.spark.read \
                .option("header", True) \
                .option("encoding", "utf-8") \
                .option("sep", ",") \
                .csv(self.path)
        except Exception as e:
            self.log(f"Erro ao ler arquivo de canais: {str(e)}")
            raise

    def load(self):
        """
        Executa o processo completo de leitura e gravação da tabela de canais.
        """
        try:
            self.log("Iniciando carga da tabela de canais na camada staging...")

            df = self.read()

            df.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable(f"{self.catalog}.{self.schema}.{self.table}")

            self.log(f"Carga concluída com sucesso para: {self.catalog}.{self.schema}.{self.table}")
        except Exception as e:
            self.log(f"Erro ao carregar dados de canais para staging: {str(e)}")
            raise

