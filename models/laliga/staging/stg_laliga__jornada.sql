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

stg_jornada AS (
    SELECT DISTINCT
        id_jornada AS id,
        id_temporada,
        jornada
    FROM base_partidos
) SELECT * FROM stg_jornada