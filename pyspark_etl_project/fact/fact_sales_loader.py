# fact/fact_sales_loader.py

from pyspark.sql.functions import col, to_date
from ingestion.base_loader import BaseLoader

class FactSalesLoader(BaseLoader):
    def load(self):
        # Leitura da staging
        df_sales = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_sales_20210726")

        # Leitura das dimensões
        dim_brand = self.spark.table("beverage_analytics.dim.dim_brand")
        dim_region = self.spark.table("beverage_analytics.dim.dim_region")
        dim_channel = self.spark.table("beverage_analytics.dim.dim_channel")

        # Transformações e joins
        fact_sales = (
            df_sales
            .withColumn("volume", col("$ volume").cast("double"))
            .withColumn("year", col("year").cast("int"))
            .withColumn("month", col("month").cast("int"))
            .withColumn("date", to_date(col("date"), "M/d/yyyy"))
            .join(dim_brand, on=["ce_brand_flvr", "brand_nm"], how="inner")
            .join(dim_region, on="btlr_org_lvl_c_desc", how="inner")
            .join(dim_channel, on="trade_chnl_desc", how="inner")
            .select(
                "date",
                "ce_brand_flvr",
                "brand_nm",
                "btlr_org_lvl_c_desc",
                "trade_chnl_desc",
                "volume",
                "year",
                "month"
            )
        )

        # Escrita particionada no Unity Catalog
        fact_sales.write \
            .mode("overwrite") \
            .format("delta") \
            .partitionBy("year", "month") \
            .saveAsTable("beverage_analytics.fact.fact_sales")

        self.log("Carga concluída com sucesso em: beverage_analytics.fact.fact_sales")