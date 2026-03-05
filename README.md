# Lead-to-Cash Data Ecosystem: Uma Abordagem End-to-End com a Modern Data Stack

Bem-vindo ao meu ecossistema de dados. Este projeto não é apenas um pipeline; é a simulação de um cenário real de engenharia de dados (Analytics Engineering) projetado para resolver um dos maiores desafios das empresas: **cruzar dados de Marketing (Meta Ads) com Vendas (KommoCRM) para descobrir o verdadeiro ROI.**

## O Contexto e o Problema de Negócio

Em ambientes reais, as empresas investem em anúncios no **Meta Ads** (Facebook/Instagram) e gerenciam seus leads no **KommoCRM**. O problema? Esses dados vivem em silos. O marketing não sabe quais leads viraram clientes pagantes, e vendas não sabe de qual campanha o cliente veio. 

**A Solução:** Construí um ecossistema completo (Lead-to-Cash) que extrai esses dados, cruza as informações no Data Warehouse e disponibiliza as métricas limpas e tratadas prontas para o consumo das áreas de negócio.

 *Nota de Arquitetura: Como não posso expor dados reais ou tokens de clientes no GitHub, eu construí do zero uma API "Mock" em FastAPI que simula perfeitamente o comportamento e os payloads reais do Meta Ads e do KommoCRM. Isso significa que você pode clonar este repositório e rodar o ecossistema inteiro na sua máquina, 100% offline e funcional.*

## O Ecossistema (Arquitetura e Stack)

Eu atuei em todas as pontas do ciclo de vida do dado, garantindo escalabilidade, governança e documentação:

1. **Simulação de Origem (Mock APIs):** FastAPI simulando os endpoints do Meta Ads e KommoCRM.
2. **Orquestração & Ingestão (Extract & Load):** O **Apache Airflow** é o maestro. Ele consome as APIs e carrega os dados brutos na camada *Bronze* do Data Warehouse.
3. **Armazenamento (Data Warehouse):** **PostgreSQL** conteinerizado atuando como nosso DW central.
4. **Transformação & Governança (Transform):** O **dbt (data build tool)** entra em cena para limpar, testar e cruzar os dados, movendo-os pelas camadas *Silver* e *Gold*. A linhagem de dados (Data Lineage) e documentação são geradas automaticamente.
5. **Data as a Service - DaaS (Serving):** Em vez de ligar um BI direto no banco, construí uma **API em FastAPI** que consome a camada *Gold* do dbt. Isso garante segurança, controle de acesso e permite que o Front-end, o BI ou cientistas de dados consumam métricas como CAC e LTV via endpoints padronizados.
6. **Infraestrutura:** Tudo orquestrado via **Docker & Docker Compose** (IaC).

## Próximos Passos (Roadmap)
- [ ] **Visualização (BI):** Plugar um Metabase ou Streamlit consumindo a API (DaaS) para gerar o dashboard final para os stakeholders.