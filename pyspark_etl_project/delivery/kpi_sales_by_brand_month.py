# delivery/kpi_sales_by_brand_month.py

from pyspark.sql.functions import col, sum as _sum, round
from ingestion.base_loader import BaseLoader

class KpiSalesByBrandMonth(BaseLoader):
    def load(self):
        catalog = "beverage_analytics"
        schema = "delivery"
        table = "kpi_sales_by_brand_month"

        # Leitura das tabelas
        df_fact_sales = self.spark.table(f"{catalog}.fact.fact_sales")
        df_dim_brand = self.spark.table(f"{catalog}.dim.dim_brand")

        # Agregação por marca e mês
        query = (
            df_fact_sales.alias("fs")
            .join(df_dim_brand.alias("b"), col("fs.ce_brand_flvr") == col("b.ce_brand_flvr"))
            .groupBy("b.brand_nm", "fs.year", "fs.month")
            .agg(round(_sum("fs.volume"), 2).alias("total_sales"))
            .orderBy("fs.year", "fs.month")
        )

        # Escrita no Unity Catalog
        query.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(f"{catalog}.{schema}.{table}")

        self.log(f"KPI gravado com sucesso em: {catalog}.{schema}.{table}")
