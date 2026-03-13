/*
  int_marketing_attribution — Camada Intermediate
  ================================================
  Enriquece cada lead Kommo com a hierarquia de campanha do Meta Ads
  via rastreamento UTM (utm_ad_id -> ad_id).

  Decisoes de design:
    - LEFT JOIN: preserva leads organicos (sem UTM) com colunas Meta como NULL.
      Excluir esses leads distorceria metricas de conversao total.

    - meta_hierarchy usa DISTINCT ON (ad_id): a tabela stg_meta_insights tem
      1 linha por (ad_id, data). Usar DISTINCT evita multiplicar leads ao
      cruzar com o historico diario — queremos so a hierarquia, nao as metricas.

    - Colunas meta_* prefixadas para diferenciar IDs vindos do Meta (confiaveis)
      dos UTMs registrados no CRM (podem ter typos ou estar desatualizados).

    - tem_atribuicao_meta: flag booleana para facilitar filtros no dashboard
      (ex: excluir leads sem rastreamento do calculo de CAC).

  Consome: stg_kommo_leads, stg_meta_insights
  Alimenta: fact_sales, fact_leads
*/

WITH leads AS (
    SELECT * FROM {{ ref('stg_kommo_leads') }}
),

-- Hierarquia de campanha deduplica por ad_id, descartando o eixo temporal.
-- DISTINCT garante 1 linha por ad independente de quantos dias de insights existem.
meta_hierarchy AS (
    SELECT DISTINCT
        ad_id,
        ad_name,
        adset_id,
        adset_name,
        campaign_id,
        campaign_name,
        objective
    FROM {{ ref('stg_meta_insights') }}
)

SELECT
    -- Identificador do lead
    l.lead_id,
    l.lead_name,

    -- Negocio
    l.preco,
    l.status_id,
    l.pipeline_id,
    l.responsible_user_id,

    -- Datas (ja em formato DATE via stg_kommo_leads)
    l.data_criacao,
    l.data_atualizacao,
    l.data_fechamento,

    -- Flags de conversao
    l.is_ganho,
    l.is_perdido,

    -- UTMs registradas no CRM (string bruta — util para auditoria de rastreamento)
    l.utm_source,
    l.utm_medium,
    l.utm_campaign_id,
    l.utm_adset_id,
    l.utm_ad_id,

    -- Hierarquia Meta validada (NULL para leads organicos/sem rastreamento valido)
    m.campaign_id   AS meta_campaign_id,
    m.campaign_name AS meta_campaign_name,
    m.adset_id      AS meta_adset_id,
    m.adset_name    AS meta_adset_name,
    m.ad_id         AS meta_ad_id,
    m.ad_name       AS meta_ad_name,
    m.objective     AS meta_objective,

    -- Flag de atribuicao: TRUE = lead rastreado com anuncio Meta valido
    (m.ad_id IS NOT NULL) AS tem_atribuicao_meta

FROM leads l
LEFT JOIN meta_hierarchy m
    ON l.utm_ad_id = m.ad_id
