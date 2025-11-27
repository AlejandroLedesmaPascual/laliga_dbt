{{
  config(
    materialized='view'
  )
}}

WITH base_partidos AS (
    SELECT
        *
    FROM {{ ref('base_laliga__partidos') }}
),

stg_temporada AS (
    SELECT DISTINCT
        id_temporada AS id,
        temporada
    FROM base_partidos
) SELECT * FROM stg_temporada