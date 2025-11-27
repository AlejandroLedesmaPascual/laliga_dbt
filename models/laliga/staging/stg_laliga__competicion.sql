{{
  config(
    materialized='view'
  )
}}

WITH base_partidos AS (
    SELECT
        *
    FROM {{ ref("base_laliga__partidos") }}
),

stg_competicion AS (
    SELECT DISTINCT
        id_competicion AS id,
        competicion AS nombre,
        id_pais
    FROM base_partidos
) SELECT * FROM stg_competicion