# ===============================================
# Arquivo: base_loader.py
# Finalidade: Classe base abstrata para loaders
# ===============================================

class BaseLoader:
    def __init__(self):
        from pyspark.sql import SparkSession
        self.spark = SparkSession.builder.getOrCreate()

    def log(self, message):
        from datetime import datetime
        print(f"[{datetime.now().isoformat()}] {message}")