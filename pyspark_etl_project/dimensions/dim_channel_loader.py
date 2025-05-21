# dimensions/dim_channel_loader.py

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader

class DimChannelLoader(BaseLoader):
    def load(self):
        df = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_channel_group_20210726")

        dim_channel = (
            df.select("TRADE_CHNL_DESC", "TRADE_GROUP_DESC", "TRADE_TYPE_DESC")
            .dropDuplicates()
        )

        dim_channel = dim_channel.toDF(*[c.lower() for c in dim_channel.columns])

        dim_channel.write.mode("overwrite").format("delta").saveAsTable("beverage_analytics.dim.dim_channel")

        self.log("Carga concluída com sucesso em: beverage_analytics.dim.dim_channel")