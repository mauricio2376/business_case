# delivery/kpi_sales_by_brand_month.py

from pyspark.sql.functions import col, sum as _sum, round
from ingestion.base_loader import BaseLoader

class KpiSalesByBrandMonth(BaseLoader):
    """
    Classe responsável pela construção do KPI de vendas por marca e mês.
    Realiza join entre fato e dimensão de marca, agrega volume e salva no Unity Catalog.
    """

    def load(self):
        """
        Executa a lógica de construção do KPI de vendas por marca e mês.
        """
        catalog = "beverage_analytics"
        schema = "delivery"
        table = "kpi_sales_by_brand_month"

        try:
            self.log("Iniciando cálculo do KPI: Vendas por marca por mês")

            # Leitura das tabelas
            df_fact_sales = self.spark.table(f"{catalog}.fact.fact_sales")
            df_dim_brand = self.spark.table(f"{catalog}.dim.dim_brand")

            # Agregação por marca e mês
            query = (
                df_fact_sales.alias("fs")
                .join(df_dim_brand.alias("b"), col("fs.ce_brand_flvr") == col("b.ce_brand_flvr"))
                .groupBy("b.brand_nm", "fs.year", "fs.month")
                .agg(round(_sum("fs.volume"), 2).alias("total_sales"))
                .orderBy(col("total_sales").desc())
            )

            # Escrita no Unity Catalog
            query.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable(f"{catalog}.{schema}.{table}")

            self.log(f"KPI gravado com sucesso em: {catalog}.{schema}.{table}")

        except Exception as e:
            self.log(f"Erro ao calcular o KPI de vendas por marca por mês: {str(e)}")
            raise
