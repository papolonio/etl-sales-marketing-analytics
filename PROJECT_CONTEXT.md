# Documento de Visão e Arquitetura: Pipeline ETL Lead-to-Cash

## 1. Visão Geral do Projeto
Este projeto é um portfólio de Engenharia de Dados de nível Sênior. O objetivo é construir um pipeline de ponta a ponta cruzando dados de Marketing (Meta Ads) com dados Comerciais (KommoCRM) para calcular métricas de negócio como CAC e ROAS.
* **Regra de Negócio Crucial:** O nível de detalhe deve ser o mais granular possível, cruzando os Leads e Vendas não apenas com a Campanha, mas com o Grupo de Anúncios (AdSet) e o Anúncio (Ad) exatos que geraram a conversão.

## 2. Restrição Arquitetural (Mock-Driven)
Para fins de portfólio, o recrutador deve conseguir rodar o projeto inteiro apenas com `docker-compose up -d`. 
NÃO usaremos chaves de API reais nem conexões externas. Toda a extração de dados será feita apontando para um serviço de **Mock API** interno (criado em FastAPI) que simula os retornos do Meta Ads e do KommoCRM. O código de ingestão deve tratar essas APIs mockadas como se fossem reais (usando paginação, auth headers, etc).

## 3. Stack Tecnológica
* Orquestração & Ingestão: Apache Airflow (Python, Requests, Orientação a Objetos)
* Armazenamento: PostgreSQL (Data Warehouse)
* Transformação: dbt Core (Arquitetura Medallion)
* Disponibilização (Data as a Service): FastAPI
* Infraestrutura: Docker & Docker Compose

## 4. Arquitetura Medallion e Regra de Cruzamento
* **Raw/Bronze:** Tabelas com os dados brutos extraídos.
* **Silver:** Dados limpos, tipados corretamente e padronizados.
* **Gold:** Tabelas fatos e dimensões focadas no negócio. 
* **O Segredo do Cruzamento (JOIN):** O mock do Meta Ads deve gerar IDs na hierarquia (Campaign -> AdSet -> Ad). O mock do KommoCRM deve gerar leads contendo campos ocultos (`custom_fields_values`) que guardam exatamente esses mesmos IDs gerados pelo Meta, simulando o rastreamento via UTMs reais para permitir o relacionamento na camada Gold.

## 5. Roteiro de Entregas (Sprints)
* **Sprint 1:** Estrutura base, Docker Compose, Postgres e o Mock API Server.
* **Sprint 2:** Airflow setup e extração POO (Mock Meta) e (Mock Kommo) salvando na Raw do Postgres.
* **Sprint 3:** Setup do dbt Core e criação das camadas Silver e Gold.
* **Sprint 4:** FastAPI para consumir a camada Gold e exibir os resultados.