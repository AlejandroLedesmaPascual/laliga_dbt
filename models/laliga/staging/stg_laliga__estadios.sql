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

stg_equipos AS (
    SELECT DISTINCT
        id_estadio AS id,
        estadio AS nombre,
        aforo
    FROM base_partidos
) SELECT * FROM stg_equipos