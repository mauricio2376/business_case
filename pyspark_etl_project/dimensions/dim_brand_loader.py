# dimensions/dim_brand_loader.py

from pyspark.sql.functions import col
from ingestion.base_loader import BaseLoader

class DimBrandLoader(BaseLoader):
    """
    Classe responsável pela carga da dimensão de marcas (brand).
    Lê os dados da camada de staging, aplica deduplicação e cast de tipos, e grava no catálogo como Delta Table.
    """

    def load(self):
        """
        Executa o processo completo de leitura da staging, transformação e escrita na camada de dimensão.
        """
        try:
            self.log("Iniciando carga da dimensão dim_brand...")

            # Leitura da tabela staging
            df = self.spark.table("beverage_analytics.staging.abi_bus_case1_beverage_sales_20210726")

            # Seleção, cast e deduplicação
            dim_brand = (
                df.select("CE_BRAND_FLVR", "BRAND_NM")
                  .dropDuplicates()
                  .withColumn("CE_BRAND_FLVR", col("CE_BRAND_FLVR").cast("int"))
            )

            # Padroniza nomes para lowercase
            dim_brand = dim_brand.toDF(*[c.lower() for c in dim_brand.columns])

            # Escrita na tabela dim
            dim_brand.write \
                .mode("overwrite") \
                .format("delta") \
                .saveAsTable("beverage_analytics.dim.dim_brand")

            self.log("Carga concluída com sucesso em: beverage_analytics.dim.dim_brand")

        except Exception as e:
            self.log(f"Erro durante a carga da dimensão dim_brand: {str(e)}")
            raise
