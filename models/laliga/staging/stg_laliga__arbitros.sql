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

stg_arbitro AS (
    SELECT DISTINCT
        id_arbitro AS id,
        arbitro AS nombre
    FROM base_partidos
) SELECT * FROM stg_arbitro