/*
  stg_kommo_leads — Camada Silver: KommoCRM
  ==========================================
  Esta e a query mais critica do pipeline: extrai os IDs do Meta Ads
  que estao aninhados dentro do array JSON custom_fields_values de cada lead.

  Estrutura do campo (gerada pelo mock server / UTM tracking real):
    [
      {"field_code": "UTM_SOURCE",      "values": [{"value": "facebook"}]},
      {"field_code": "UTM_MEDIUM",      "values": [{"value": "cpc"}]},
      {"field_code": "UTM_CAMPAIGN_ID", "values": [{"value": "120210000000001"}]},
      {"field_code": "UTM_ADSET_ID",    "values": [{"value": "120220000000001"}]},
      {"field_code": "UTM_AD_ID",       "values": [{"value": "120230000000001"}]}
    ]

  Estrategia de extracao:
    1. CTE leads_with_jsonb: converte o TEXT para JSONB com tratamento
       de valores nulos/vazios (NULL, '', 'null', '[]').
    2. CTE utm_fields: usa LEFT JOIN LATERAL + jsonb_array_elements para
       expandir o array e pivota os valores com MAX(CASE WHEN).
       O LEFT JOIN garante que leads sem UTM (organicos) permanecem no resultado.
    3. Query final: junta tudo, converte timestamps Unix para DATE e
       adiciona flags booleanas de conversao.

  Chave de JOIN com stg_meta_insights: utm_ad_id = ad_id
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'kommo_leads') }}
),

-- Passo 1: Normaliza o campo JSON para JSONB, tratando todos os casos de valor invalido
leads_with_jsonb AS (
    SELECT
        *,
        CASE
            WHEN custom_fields_values IS NULL
              OR TRIM(custom_fields_values) IN ('', 'null', '[]')
            THEN '[]'::jsonb
            ELSE custom_fields_values::jsonb
        END AS custom_fields_jsonb
    FROM source
),

-- Passo 2: Expande o array e pivota os campos UTM em colunas
-- LEFT JOIN LATERAL garante que leads com array vazio mantem uma linha (com elem = NULL)
utm_fields AS (
    SELECT
        l.id,
        MAX(
            CASE WHEN elem->>'field_code' = 'UTM_CAMPAIGN_ID'
            THEN elem->'values'->0->>'value' END
        ) AS utm_campaign_id,

        MAX(
            CASE WHEN elem->>'field_code' = 'UTM_ADSET_ID'
            THEN elem->'values'->0->>'value' END
        ) AS utm_adset_id,

        MAX(
            CASE WHEN elem->>'field_code' = 'UTM_AD_ID'
            THEN elem->'values'->0->>'value' END
        ) AS utm_ad_id,

        MAX(
            CASE WHEN elem->>'field_code' = 'UTM_SOURCE'
            THEN elem->'values'->0->>'value' END
        ) AS utm_source,

        MAX(
            CASE WHEN elem->>'field_code' = 'UTM_MEDIUM'
            THEN elem->'values'->0->>'value' END
        ) AS utm_medium

    FROM leads_with_jsonb l
    LEFT JOIN LATERAL jsonb_array_elements(l.custom_fields_jsonb) AS elem ON TRUE
    GROUP BY l.id
)

-- Passo 3: Query final — campos limpos e tipados + UTMs extraidas
SELECT
    s.id                                AS lead_id,
    s.name                              AS lead_name,
    s.price::NUMERIC(12, 2)            AS preco,
    s.status_id,
    s.pipeline_id,
    s.responsible_user_id,
    s.loss_reason_id,

    -- Conversao de Unix Timestamp para DATE
    -- TO_TIMESTAMP retorna NULL quando a coluna e NULL (closed_at de leads abertos)
    TO_TIMESTAMP(s.created_at)::DATE    AS data_criacao,
    TO_TIMESTAMP(s.updated_at)::DATE    AS data_atualizacao,
    TO_TIMESTAMP(s.closed_at)::DATE     AS data_fechamento,

    -- Flags de conversao baseadas nos status especiais do Kommo
    -- 142 = Ganho (Won) | 143 = Perdido (Lost)
    (s.status_id = 142)                 AS is_ganho,
    (s.status_id = 143)                 AS is_perdido,

    -- UTMs extraidas do JSON — chaves de JOIN com a hierarquia do Meta Ads
    u.utm_campaign_id,
    u.utm_adset_id,
    u.utm_ad_id,       -- chave principal de JOIN: stg_kommo_leads.utm_ad_id = stg_meta_insights.ad_id
    u.utm_source,
    u.utm_medium

FROM leads_with_jsonb s
LEFT JOIN utm_fields u ON s.id = u.id
