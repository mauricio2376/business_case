# dimensions/dim_brand_loader.py

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader

class DimBrandLoader(BaseLoader):
    def load(self):
        df = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_sales_20210726")

        dim_brand = (
            df.select("CE_BRAND_FLVR", "BRAND_NM")
            .dropDuplicates()
            .withColumn("CE_BRAND_FLVR", col("CE_BRAND_FLVR").cast("int"))
        )

        dim_brand = dim_brand.toDF(*[c.lower() for c in dim_brand.columns])

        dim_brand.write.mode("overwrite").format("delta").saveAsTable("beverage_analytics.dim.dim_brand")

        self.log("Carga concluída com sucesso em: beverage_analytics.dim.dim_brand")