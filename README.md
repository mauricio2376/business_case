# Projeto de Engenharia de Dados - Business Case

Este projeto foi desenvolvido com o objetivo de estruturar uma pipeline de ingestão, transformação e carga de dados sobre vendas de bebidas, utilizando o Databricks, Delta Lake, Unity Catalog e PySpark orientado a objetos.

## Estrutura Geral

```
project-root/
|
|-- staging/                                            # Carga de dados brutos
|   |-- staging_channel_loader.py
|   |-- staging_sales_loader.py
|
|-- dimensions/                                         # Criação de tabelas de dimensão
|   |-- dim_brand_loader.py
|   |-- dim_region_loader.py
|   |-- dim_channel_loader.py
|
|-- fact/                                               # Criação da tabela fato
|   |-- fact_sales_loader.py
|
|-- delivery/                                           # Criação dos KPIs
|   |-- kpi_top3_groups_by_region.py
|   |-- kpi_sales_by_brand_month.py
|   |-- kpi_lowest_brand_by_region.py
|
|-- ingestion/                                          # Classe base reutilizável
|   |-- base_loader.py
|
|-- main.py                                             # Orquestração geral das cargas
|-- README.md                                           # Este arquivo
```

## Organização do Unity Catalog

```
beverage_analytics/
|
|-- delivery/                                           # Tabela de KPIs  
|   |-- kpi_top3_groups_by_region
|   |-- kpi_sales_by_brand_month
|   |-- kpi_lowest_brand_by_region
|
|-- dim/                                                # Tabelas de dimensão
|   |-- dim_brand
|   |-- dim_channel
|   |-- dim_region
|
|-- fact/                                               # Tabela fato
|   |-- fact_sales
|
|-- staging/                                            # Carga de dados brutos 
|   |-- abi_bus_case1_beverage_channel_group_20210726
|   |-- abi_bus_case1_beverage_sales_20210726
|
```

## Requisitos

- Databricks Runtime com suporte a Delta Lake
- Unity Catalog habilitado
- Tabelas de staging previamente criadas:
  - `beverage_analytics.staging.abi_bus_case1_beverage_sales_20210726`
  - `beverage_analytics.staging.abi_bus_case1_beverage_channel_group_20210726`

## Execução do pipeline

Basta executar o script principal no Databricks:

```python
%run main.py
```

## Logging

Todo o projeto utiliza logging centralizado para acompanhamento de status de cada etapa.

## Comentários e estilo

Todos os scripts seguem o padrão de documentação Google Style para classes e métodos.

## KPIs Gerados

- `kpi_top3_groups_by_region`: Top 3 grupos comerciais por região
- `kpi_sales_by_brand_month`: Vendas por marca por mês
- `kpi_lowest_brand_by_region`: Marca com menor volume por região

## Observações

- Todas as tabelas são salvas em formato Delta Lake no Unity Catalog.
- As partições são feitas por `year` e `month` na tabela fato.

## Autor

Mauricio Andrade
