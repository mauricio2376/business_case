# delivery/kpi_lowest_brand_by_region.py

from pyspark.sql.functions import col, sum as _sum, round, row_number
from pyspark.sql.window import Window
from ingestion.base_loader import BaseLoader

class KpiLowestBrandByRegion(BaseLoader):
    """
    Classe responsável pela construção do KPI de menor volume de vendas por marca e região.
    Realiza a agregação das vendas por marca e região, determina a menor venda com função de janela e salva no Unity Catalog.
    """

    def load(self):
        """
        Executa a lógica de construção do KPI de menor volume de vendas por marca e região.
        """
        catalog = "beverage_analytics"
        schema = "delivery"
        table = "kpi_lowest_brand_by_region"

        try:
            self.log("Iniciando cálculo do KPI: Menor volume por marca e região")

            # Leitura da tabela fato
            df_fact_sales = self.spark.table(f"{catalog}.fact.fact_sales")

            # Agregação por marca e região
            query = (
                df_fact_sales
                .groupBy("brand_nm", "btlr_org_lvl_c_desc")
                .agg(round(_sum("volume"), 2).alias("total_sales"))
            )

            # Determina o menor valor por região
            windowSpec = Window.partitionBy("btlr_org_lvl_c_desc").orderBy(col("total_sales").asc())
            lowest = query.withColumn("rank", row_number().over(windowSpec)).filter("rank = 1")
            selected_columns = lowest.select(
                                            "brand_nm",
                                            "btlr_org_lvl_c_desc",
                                            "total_sales"
                                            )

            # Escrita no Unity Catalog
            selected_columns.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable(f"{catalog}.{schema}.{table}")

            self.log(f"KPI gravado com sucesso em: {catalog}.{schema}.{table}")

        except Exception as e:
            self.log(f"Erro ao calcular o KPI de menor volume por região: {str(e)}")
            raise
