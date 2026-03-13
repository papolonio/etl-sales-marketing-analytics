/*
  dim_salesperson — Dimensao de Vendedores
  ==========================================
  Uma linha por vendedor/usuario responsavel.
  Chave de JOIN com fact_sales e fact_leads via responsible_user_id.

  Consome: stg_kommo_users
*/

SELECT
    user_id         AS salesperson_id,
    user_name       AS salesperson_name

FROM {{ ref('stg_kommo_users') }}
