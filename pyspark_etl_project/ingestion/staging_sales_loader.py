# staging_sales_loader.py
# ===============================================
# Finalidade: Carregar staging de vendas (sales)
# ===============================================

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader
from pyspark.sql import DataFrame

class StagingSalesLoader(BaseLoader):
    """
    Loader responsável por carregar os dados de vendas para a camada de staging.
    """

    def __init__(self):
        """
        Inicializa o loader com caminho de origem e metadados de catálogo.
        """
        super().__init__()
        self.catalog = "beverage_analytics"
        self.schema = "staging"
        self.table = "abi_bus_case1_beverage_sales_20210726"
        self.path = "/FileStore/bronze/abi_bus_case1_beverage_sales_20210726.csv"

    def read(self) -> DataFrame:
        """
        Lê o arquivo CSV da base de vendas com tratamento do campo de volume.

        Returns:
            DataFrame: Dados de vendas lidos e tratados.
        """
        try:
            self.log(f"Lendo arquivo CSV de vendas: {self.path}")
            df = self.spark.read \
                .option("header", True) \
                .option("encoding", "utf-8") \
                .option("sep", "\t") \
                .csv(self.path)

            df = df.withColumn("Volume", col("$ Volume").cast("double")).drop("$ Volume")
            return df

        except Exception as e:
            self.log(f"Erro ao ler arquivo de vendas: {str(e)}")
            raise

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Retorna os dados conforme lidos, sem transformações adicionais.

        Args:
            df (DataFrame): Dados brutos lidos do CSV.

        Returns:
            DataFrame: Dados prontos para escrita na staging.
        """
        self.log("Nenhuma transformação aplicada para staging de vendas.")
        return df
