# ===============================================
# Arquivo: base_loader.py
# Finalidade: Classe base abstrata para loaders
# ===============================================

from abc import ABC, abstractmethod

class BaseLoader(ABC):
    def __init__(self, spark, catalog, schema, table):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table = table

    def full_table_name(self):
        return f"{self.catalog}.{self.schema}.{self.table}"

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def transform(self, df):
        pass

    def write(self, df):
        df.write \
            .mode("overwrite") \
            .format("delta") \
            .saveAsTable(self.full_table_name())
        print(f"Carga concluída com sucesso: {self.full_table_name()}")

    def run(self):
        df = self.read()
        df_transformed = self.transform(df)
        self.write(df_transformed)
