# ===============================================
# Arquivo: base_loader.py
# Finalidade: Classe base abstrata para loaders
# ===============================================

class BaseLoader:
    """
    Classe base para os loaders de dados.

    Fornece sessão Spark e método utilitário de logging para uso em classes derivadas.
    """

    def __init__(self):
        """
        Inicializa a sessão Spark com um appName definido.
        """
        from pyspark.sql import SparkSession
        self.spark = SparkSession.builder.appName("business_case_sales").getOrCreate()

    def log(self, message):
        """
        Imprime uma mensagem de log com timestamp no formato ISO 8601.

        Args:
            message (str): A mensagem a ser exibida no log.
        """
        from datetime import datetime
        print(f"[{datetime.now().isoformat()}] {message}")