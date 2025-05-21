# dimensions/dim_region_loader.py

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader

class DimRegionLoader(BaseLoader):
    def load(self):
        df = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_sales_20210726")

        dim_region = df.select("BTLR_ORG_LVL_C_DESC").dropDuplicates()

        dim_region = dim_region.toDF(*[c.lower() for c in dim_region.columns])

        dim_region.write.mode("overwrite").format("delta").saveAsTable("beverage_analytics.dim.dim_region")

        self.log("Carga concluída com sucesso em: beverage_analytics.dim.dim_region")