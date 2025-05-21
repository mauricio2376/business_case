# main.py - Orquestrador de Cargas

from dimensions.dim_brand_loader import DimBrandLoader
from dimensions.dim_region_loader import DimRegionLoader
from dimensions.dim_channel_loader import DimChannelLoader
from fact.fact_sales_loader import FactSalesLoader
from delivery.kpi_top3_groups_by_region import KpiTop3GroupsByRegion
from delivery.kpi_sales_by_brand_month import KpiSalesByBrandMonth
from delivery.kpi_lowest_brand_by_region import KpiLowestBrandByRegion
import logging

# ------------------------------------------------------------------------------
# Script principal de orquestração de cargas de dados
# Este módulo executa sequencialmente as etapas de carregamento das dimensões,
# da tabela fato e dos KPIs calculados, utilizando classes que encapsulam
# a lógica de leitura, transformação e escrita no Unity Catalog (Delta Lake).
# ------------------------------------------------------------------------------

# Configuração de logging centralizado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PipelineLogger")

if __name__ == "__main__":
    logger.info("Iniciando orquestração de cargas...")

    try:
        # ====================
        # 1. Carregamento das Dimensões
        # ====================
        DimBrandLoader().load()
        DimRegionLoader().load()
        DimChannelLoader().load()

        # ====================
        # 2. Carregamento da Tabela Fato
        # ====================
        FactSalesLoader().load()

        # ====================
        # 3. Cálculo e carga dos KPIs
        # ====================
        KpiTop3GroupsByRegion().load()
        KpiSalesByBrandMonth().load()
        KpiLowestBrandByRegion().load()

        logger.info("Pipeline finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante execução do pipeline: {str(e)}")
        raise
