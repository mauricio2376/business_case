# Projeto de Engenharia de Dados - Business Case

Este projeto foi desenvolvido com o objetivo de estruturar uma pipeline completa para ingestão, transformação e análise de dados de vendas de bebidas utilizando o Databricks, PySpark, Delta Lake e Unity Catalog. O projeto é orientado a objetos e modularizado para facilitar manutenção e escalabilidade.

---

## Índice

1. [Arquitetura Geral](#arquitetura-geral)
2. [Organização das Tabelas no Unity Catalog](#organização-das-tabelas-no-unity-catalog)
3. [Fluxo do Dado](#fluxo-do-dado)
4. [Estrutura de Diretórios e Códigos](#estrutura-de-diretórios-e-códigos)
5. [Execução do Pipeline](#execução-do-pipeline)
6. [Logging e Monitoramento](#logging-e-monitoramento)
7. [Padrão de Codificação](#padrão-de-codificação)
8. [KPIs Gerados](#kpis-gerados)
9. [Observações Finais](#observações-finais)
10. [Autor](#autor)

---

## Arquitetura Geral

A arquitetura do projeto contempla as seguintes camadas:

- **Staging (Bronze)**: ingestão de dados brutos CSV
- **Dimensões (Silver)**: curadoria de dados referenciais
- **Fato (Gold)**: consolidação de transações
- **Delivery**: geração de tabelas derivadas com indicadores (KPIs)

O armazenamento é feito em Delta Tables, com gerenciamento via Unity Catalog. O código é modular e organizado em pacotes.

## Organização das Tabelas no Unity Catalog

```
CATALOG: beverage_analytics
├── SCHEMA: staging
│   ├── abi_bus_case1_beverage_sales_20210726
│   └── abi_bus_case1_beverage_channel_group_20210726
├── SCHEMA: dim
│   ├── dim_brand
│   ├── dim_region
│   └── dim_channel
├── SCHEMA: fact
│   └── fact_sales
├── SCHEMA: delivery
│   ├── kpi_top3_groups_by_region
│   ├── kpi_sales_by_brand_month
│   └── kpi_lowest_brand_by_region
```

## Fluxo do Dado

1. **Staging:** arquivos CSV são lidos diretamente do DBFS e salvos em tabelas brutas (`staging`).
2. **Dimensões:** dados referenciais são extraídos da staging com padronização de tipos e colunas (lowercase).
3. **Fato:** dados de vendas são transformados e integrados às dimensões para formar a tabela `fact_sales`.
4. **Delivery:** métricas de negócio são calculadas com base na fato e salvas em tabelas finais de análise.

## Estrutura de Diretórios e Códigos

```
project-root/
|
|-- staging/                      # Carga de dados brutos
|   |-- staging_channel_loader.py
|   |-- staging_sales_loader.py
|
|-- dimensions/                  # Criação de tabelas de dimensão
|   |-- dim_brand_loader.py
|   |-- dim_region_loader.py
|   |-- dim_channel_loader.py
|
|-- fact/                        # Criação da tabela fato
|   |-- fact_sales_loader.py
|
|-- delivery/                    # Criação dos KPIs
|   |-- kpi_top3_groups_by_region.py
|   |-- kpi_sales_by_brand_month.py
|   |-- kpi_lowest_brand_by_region.py
|
|-- ingestion/                   # Classe base reutilizável
|   |-- base_loader.py
|
|-- main.py                      # Orquestração geral das cargas
|-- README.md                    # Esta documentação
```

### Objetivo de cada módulo
- **base_loader.py**: classe abstrata que define o esqueleto dos loaders.
- **staging_channel_loader.py / staging_sales_loader.py**: lê arquivos CSV e salva no schema `staging`.
- **dim_*.py**: cria dimensões padronizadas a partir da staging.
- **fact_sales_loader.py**: realiza joins entre staging e dimensões para formar a tabela fato.
- **kpi_*.py**: realiza agregações e cálculos de indicadores de negócio (KPIs).
- **main.py**: ponto de entrada para orquestração sequencial das etapas.

## Execução do Pipeline

Execute o script principal no Databricks para rodar toda a orquestração:

```python
%run main.py
```

Ou, individualmente, execute cada script de forma modular.

## Logging e Monitoramento

Todos os scripts fazem uso da biblioteca `logging` com formatação padrão e níveis `INFO` e `ERROR`, permitindo rastreamento da execução e depuração.

## Padrão de Codificação

- Google Style para docstrings de classes e métodos
- Estrutura orientada a objetos reutilizável
- Colunas padronizadas com lowercase

## KPIs Gerados

1. **kpi_top3_groups_by_region**: Top 3 grupos comerciais por região com maior volume.
2. **kpi_sales_by_brand_month**: Total de vendas por marca e mês.
3. **kpi_lowest_brand_by_region**: Marca com menor volume por região.

## Observações Finais

- A tabela `fact_sales` é particionada por `year` e `month`.
- Todas as tabelas são formatadas como Delta Tables.
- O Unity Catalog foi utilizado para governança centralizada.
- Os arquivos foram carregados inicialmente via DBFS.

## Autor

Mauricio Andrade
