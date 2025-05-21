# delivery/kpi_lowest_brand_by_region.py

from pyspark.sql.functions import col, sum as _sum, round, row_number
from pyspark.sql.window import Window
from ingestion.base_loader import BaseLoader

class KpiLowestBrandByRegion(BaseLoader):
    def load(self):
        catalog = "beverage_analytics"
        schema = "delivery"
        table = "kpi_lowest_brand_by_region"

        # Leitura da fato
        df_fact_sales = self.spark.table(f"{catalog}.fact.fact_sales")

        # Agregação por marca e região
        query = (
            df_fact_sales
            .groupBy("brand_nm", "btlr_org_lvl_c_desc")
            .agg(round(_sum("volume"), 2).alias("total_sales"))
        )

        windowSpec = Window.partitionBy("btlr_org_lvl_c_desc").orderBy(col("total_sales").asc())
        lowest = query.withColumn("rank", row_number().over(windowSpec)).filter("rank == 1")

        # Escrita no Unity Catalog
        lowest.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(f"{catalog}.{schema}.{table}")

        self.log(f"KPI gravado com sucesso em: {catalog}.{schema}.{table}")