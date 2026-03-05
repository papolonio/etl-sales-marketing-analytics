# 📊 Lead-to-Cash Analytics Pipeline: Unificando Marketing e Vendas



Um projeto de Engenharia de Dados *end-to-end* desenvolvido para solucionar um dos problemas mais complexos de operações financeiras e de marketing: **a quebra de rastreabilidade (atribuição) entre o clique no anúncio e a venda fechada.**

## O Problema de Negócio

Em arquiteturas tradicionais, os dados operacionais vivem em silos:
1. **Marketing (Meta Ads):** Sabe quanto gastou, quantos cliques gerou e o Custo por Clique (CPC). Mas não sabe se o lead realmente comprou algo.
2. **Vendas (Kommo CRM):** Sabe quem comprou e qual foi a receita gerada. Mas não sabe de qual anúncio ou campanha aquele cliente veio.

**A Consequência:** A empresa queima dinheiro escalando campanhas que geram leads baratos, mas que nunca convertem em vendas reais, distorcendo o Retorno sobre Investimento (ROI).

## O Impacto da Solução (Business Value)

Este pipeline cruza os dados de ambas as pontas, implementando a esteira de **Lead-to-Cash**, permitindo ao time de *Growth* e Diretoria responder em tempo real:
* **ROAS Real (Return on Ad Spend):** Qual anúncio específico gerou a maior receita de vendas fechadas?
* **CAC Preciso (Customer Acquisition Cost):** Quanto custou, no nível da campanha, adquirir um cliente pagante?
* **Otimização de Orçamento:** Possibilidade de pausar anúncios de "falsa performance" e realocar orçamento para campanhas de alta conversão.

---

## Arquitetura Técnica e Desafios Resolvidos

O projeto foi desenhado sob a **Arquitetura Medallion** (Bronze, Silver, Gold), focado em modularidade, resiliência e disponibilização via **Data as a Service (DaaS)**.

### 1. Ingestão (Airflow + Python POO)
* **Desafio:** APIs RESTful geralmente possuem paginação, rate limits e retornos complexos.
* **Solução:** Construção de *API Clients* em Python utilizando **Programação Orientada a Objetos (POO)**. As DAGs do Apache Airflow atuam apenas como orquestradoras (separação de responsabilidades), extraindo os dados em estado bruto (Raw/Bronze) de forma idempotente para o PostgreSQL.

### 2. Transformação e Modelagem (dbt Core + PostgreSQL)
* **Desafio:** O Kommo CRM retorna as UTMs (parâmetros de rastreio de marketing) "escondidas" e aninhadas dentro de um array JSON complexo (`custom_fields_values`).
* **Solução:** Utilização de modelagem dimensional via **dbt (Data Build Tool)**. Na camada *Silver*, apliquei técnicas avançadas de SQL no PostgreSQL (como `LEFT JOIN LATERAL jsonb_array_elements`) para fazer o *unnest* e o *pivot* do JSON, transformando chaves dinâmicas em colunas tabulares estruturadas (`utm_campaign_id`, `utm_ad_id`).
* **Gold Layer:** Construção da `fato_roi_marketing`, cruzando os dados de custo do Meta Ads com os dados de conversão e receita do CRM.

### 3. Disponibilização: Data as a Service (FastAPI)
* **Desafio:** Conectar ferramentas de BI ou Front-ends diretamente ao Data Warehouse gera riscos de segurança, gargalos de conexão e forte acoplamento.
* **Solução:** Desenvolvimento de uma API Restful de leitura com **FastAPI**, SQLAlchemy e Pydantic. Ela serve os dados agregados da camada Gold de forma rápida, tipada e paginada, isolando o banco de dados analítico do consumidor final.

---

## Stack Tecnológica

* **Orquestração:** Apache Airflow
* **Transformação & Data Quality:** dbt (Data Build Tool)
* **Data Warehouse:** PostgreSQL
* **DaaS / API:** FastAPI, Uvicorn, SQLAlchemy
* **Linguagem:** Python 3.11+
* **Infraestrutura:** Docker & Docker Compose

---

## Como executar este projeto localmente

**1. Clone o repositório:**
```bash
git clone [https://github.com/SEU_USUARIO/etl-sales-marketing-analytics.git](https://github.com/SEU_USUARIO/etl-sales-marketing-analytics.git)
cd etl-sales-marketing-analytics