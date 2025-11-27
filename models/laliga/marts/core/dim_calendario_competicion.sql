{{
  config(
    materialized='table'
  )
}}

WITH int_jornada AS (
    SELECT
        *
    FROM {{ ref('int_jornada__enriquecimiento') }}
),

stg_temporada AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__temporadas') }}
),

dim_calendario_competicion AS (
    SELECT
        j.id,
        jornada,
        vuelta,
        temporada
    FROM int_jornada j
    JOIN stg_temporada t
    ON j.id_temporada = t.id
) SELECT * FROM dim_calendario_competicion