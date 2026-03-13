/*
  fact_sales — Camada Gold: Fato de Vendas
  ==========================================
  Granularidade: 1 linha por venda ganha (is_ganho = TRUE / status_id = 142).

  Esta tabela e a base primaria do dashboard executivo:
    - Visao Executiva  : faturamento total, ticket medio, numero de vendas
    - Ranking Vendedores: GROUP BY responsible_user_id
    - Run Rate         : GROUP BY mes_venda para acumulado mensal vs. meta
    - Marketing & Funil: conversao por campanha, receita atribuida por anuncio

  Colunas de relacionamento para dimensoes:
    - responsible_user_id -> dim_salesperson  (nome do vendedor)
    - campaign_id         -> dim_campaign     (nome e objetivo da campanha)

  Decisoes de design:
    - Filtro is_ganho no CTE de atribuicao (nao no WHERE final) para deixar
      o plano de execucao mais explicito para quem ler o SQL.

    - mes_venda: DATE_TRUNC('month', ...) retorna o primeiro dia do mes
      (ex: 2026-03-01). Padrao conveniente para GROUP BY mensal e JOIN
      com fact_targets (que tambem usa primeiro dia do mes como chave).

    - LEFT JOIN com int_sales_funnel: toda venda deve ter days_to_close,
      mas o LEFT garante que vendas sem dados de funil nao sejam perdidas.
*/

WITH vendas AS (
    -- Apenas leads ganhos, ja enriquecidos com hierarquia Meta
    SELECT * FROM {{ ref('int_marketing_attribution') }}
    WHERE is_ganho
),

funil AS (
    -- Metricas de ciclo de vida — filtradas para ganhos por performance
    SELECT
        lead_id,
        days_to_close,
        ciclo_bucket
    FROM {{ ref('int_sales_funnel') }}
    WHERE is_ganho
)

SELECT
    -- Chaves de negocio
    v.lead_id,
    v.responsible_user_id,
    v.meta_campaign_id      AS campaign_id,
    v.meta_adset_id         AS adset_id,
    v.meta_ad_id            AS ad_id,

    -- Datas
    v.data_criacao          AS data_lead,
    v.data_fechamento       AS data_venda,
    DATE_TRUNC('month', v.data_fechamento)::DATE AS mes_venda,

    -- Receita
    v.preco                 AS receita,

    -- Ciclo de vida
    f.days_to_close,
    f.ciclo_bucket,

    -- Contexto desnormalizado para display direto no dashboard (evita JOINs no BI)
    v.meta_campaign_name    AS campaign_name,
    v.meta_adset_name       AS adset_name,
    v.meta_ad_name          AS ad_name,
    v.meta_objective        AS campaign_objective,
    v.utm_source,
    v.utm_medium,
    v.tem_atribuicao_meta

FROM vendas v
LEFT JOIN funil f
    ON v.lead_id = f.lead_id

ORDER BY v.data_fechamento DESC
