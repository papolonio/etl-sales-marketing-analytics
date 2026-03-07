/*
  stg_kommo_users — Camada Silver: Vendedores / Usuarios Kommo
  =============================================================
  Renomeia id -> user_id e name -> user_name para padronizacao
  de nomenclatura e evitar conflito com palavras reservadas em SQL.

  Consome: raw.kommo_users
  Alimenta: dim_salesperson
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'kommo_users') }}
)

SELECT
    id   AS user_id,
    name AS user_name

FROM source
