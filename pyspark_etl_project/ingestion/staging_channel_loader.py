# ===============================================
# Arquivo: staging_channel_loader.py
# Finalidade: Carregar staging de canais (channel group)
# ===============================================

from ingestion.base_loader import BaseLoader

class StagingChannelLoader(BaseLoader):
    def __init__(self, spark):
        super().__init__(spark, catalog="beverage_analytics", schema="staging", table="abi_bus_case1_beverage_channel_group_20210726")
        self.path = "/FileStore/bronze/abi_bus_case1_beverage_channel_group_20210726.csv"

    def read(self):
        print(f"Lendo arquivo CSV de canais: {self.path}")
        return self.spark.read.option("header", True).option("encoding", "utf-8").option("sep", ",").csv(self.path)

    def transform(self, df):
        return df  # staging não precisa de transformação no momento
