# Lead-to-Cash Data Ecosystem: End-to-End Analytics Engineering

Bem-vindo ao meu ecossistema de dados. Este projeto não é apenas um pipeline; é a simulação de um cenário real de Engenharia de Dados e Analytics Engineering projetado para resolver um dos maiores desafios das empresas: **cruzar dados de Marketing (Meta Ads) com Vendas (KommoCRM) para descobrir o verdadeiro ROI.**

![Arquitetura do Projeto](assets/arquitetura.png) 

## O Contexto e o Problema de Negócio

Em ambientes reais, as empresas investem milhares em anúncios no **Meta Ads** (Facebook/Instagram) e gerenciam a conversão desses leads no **KommoCRM**. O problema clássico? Esses dados vivem em silos. O marketing não sabe quais leads viraram clientes pagantes (e quanto geraram de receita), e o time de vendas não sabe de qual campanha o cliente veio. 

**A Solução:** Construí um ecossistema completo (Lead-to-Cash) que extrai esses dados, cruza as informações no Data Warehouse e disponibiliza as métricas limpas e tratadas prontas para o consumo das áreas de negócio.

 *Nota de Arquitetura: Como não posso expor dados reais ou tokens de clientes em um repositório público, eu construí do zero uma API "Mock" em FastAPI que simula perfeitamente o comportamento e os payloads reais do Meta Ads e do KommoCRM. Isso significa que você pode clonar este repositório e rodar o ecossistema inteiro na sua máquina, 100% offline e reprodutível.*

---

## Entendendo o Fluxo End-to-End (Arquitetura Detalhada)

O ecossistema foi desenhado seguindo as melhores práticas da *Modern Data Stack*, cobrindo todo o ciclo de vida do dado da esquerda para a direita:

1. **Data Sources (Origens Simuladas):** **FastAPI** gerando dados paginados, tokens e payloads complexos do Meta e Kommo.
2. **Orchestration & Ingestion (Extract & Load):** **Apache Airflow** agendando a extração diária das APIs e carregando os dados brutos de forma incremental.
3. **Data Warehouse (Armazenamento):** **PostgreSQL** recebendo os dados brutos na sua **Camada Bronze (Raw Data)**.
4. **Transformation & Data Quality (dbt):** O **dbt** atua no DW, limpando os dados para a **Camada Silver** e cruzando Marketing/Vendas na **Camada Gold**. Aplica testes de qualidade e gera o catálogo de dados.
5. **Serving (Data as a Service - DaaS):** Uma **API dedicada (FastAPI)** lê a Camada Gold e expõe métricas prontas (CAC, LTV, ROAS) de forma segura, evitando que o BI sobrecarregue o banco de dados.
6. **Infraestrutura como Código (IaC):** Tudo orquestrado via **Docker & Docker Compose**.

---

## Por Dentro do Ecossistema (Showcase)

Aqui estão os bastidores do pipeline em funcionamento:

### 1. Orquestração (Apache Airflow)
Extração resiliente lidando com paginação e limites de requisição das APIs.

![Airflow DAGs](assets/airflow_dag.png) 
*Visão do Airflow orquestrando o pipeline de ingestão e transformação.*

### 2. Transformação e Linhagem de Dados (dbt Docs)
Arquitetura medalhão aplicada. Rastreabilidade total do dado, da origem ao indicador de negócio.

![dbt Data Lineage](assets/dbt_lineage.png)
*Grafo de linhagem (Lineage Graph) gerado automaticamente pelo dbt.*

### 3. Disponibilização DaaS (FastAPI Swagger)
Endpoints documentados e prontos para o consumo das equipes de Produto e Analistas de BI.

![FastAPI Swagger](assets/api_swagger.png)
*Interface Swagger mostrando os endpoints de consumo da Camada Gold.*

