/*
  stg_meta_insights — Camada Silver: Meta Ads
  ============================================
  Responsabilidades:
    1. Cast de tipos: todos os campos numericos chegam como TEXT da API (padrao Graph API).
       Aqui garantimos NUMERIC para spend e INTEGER para contadores.
    2. Renomeio semantico: date_start -> data_ref (padrao do projeto).
    3. Manutencao da hierarquia completa: campaign_id > adset_id > ad_id
       para suportar agregacoes em qualquer nivel no Gold.
    4. actions permanece como TEXT (JSON): o parsing granular por action_type
       ocorre apenas se necessario na camada Gold.
*/

WITH source AS (
    SELECT * FROM "data_warehouse"."raw"."meta_insights"
)

SELECT
    -- Identificadores de conta
    account_id,
    account_name,

    -- Hierarquia de campanha (chave de JOIN com o Kommo via UTMs)
    campaign_id,
    campaign_name,
    adset_id,
    adset_name,
    ad_id,
    ad_name,
    objective,

    -- Metricas financeiras: chegam como TEXT da Graph API -> cast para NUMERIC
    spend::NUMERIC(12, 2)       AS spend,

    -- Metricas de engajamento: cast para INTEGER
    clicks::INTEGER             AS clicks,
    inline_link_clicks::INTEGER AS inline_link_clicks,
    impressions::INTEGER        AS impressions,

    -- Dimensao temporal
    date_start::DATE            AS data_ref,

    -- Array de acoes (lead, link_click, page_view, etc.) — mantido como JSON
    -- para flexibilidade na camada Gold sem reprocessar o Silver
    actions

FROM source