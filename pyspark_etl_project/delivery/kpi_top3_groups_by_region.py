# delivery/kpi_top3_groups_by_region.py

from pyspark.sql.functions import col, row_number, sum as _sum, round
from pyspark.sql.window import Window
from ingestion.base_loader import BaseLoader

class KpiTop3GroupsByRegion(BaseLoader):
    """
    Classe responsável pela construção do KPI de Top 3 grupos comerciais por região.
    Realiza join entre fatos e dimensões, agrega vendas e salva o resultado em Delta Table.
    """

    def load(self):
        """
        Executa a lógica de construção do KPI de Top 3 grupos comerciais por região.
        """
        catalog = "beverage_analytics"
        schema = "delivery"
        table = "kpi_top3_groups_by_region"

        try:
            self.log("Iniciando cálculo do KPI: Top 3 grupos comerciais por região")

            # Leitura das tabelas
            df_fact_sales = self.spark.table(f"{catalog}.fact.fact_sales")
            df_dim_channel = self.spark.table(f"{catalog}.dim.dim_channel")
            df_dim_region = self.spark.table(f"{catalog}.dim.dim_region")

            # Agregação e rankeamento
            query = (
                df_fact_sales.alias("fs")
                .join(df_dim_channel.alias("ch"), col("fs.trade_chnl_desc") == col("ch.trade_chnl_desc"))
                .join(df_dim_region.alias("region"), col("fs.btlr_org_lvl_c_desc") == col("region.btlr_org_lvl_c_desc"))
                .groupBy("region.btlr_org_lvl_c_desc", "ch.trade_group_desc")
                .agg(round(_sum("fs.volume"), 2).alias("total_sales"))
            )

            windowSpec = Window.partitionBy("btlr_org_lvl_c_desc").orderBy(col("total_sales").desc())
            top3 = query.withColumn("rank", row_number().over(windowSpec)).filter("rank <= 3")

            # Escrita no Unity Catalog
            top3.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable(f"{catalog}.{schema}.{table}")

            self.log(f"KPI gravado com sucesso em: {catalog}.{schema}.{table}")

        except Exception as e:
            self.log(f"Erro ao calcular o KPI Top 3 grupos comerciais por região: {str(e)}")
            raise
