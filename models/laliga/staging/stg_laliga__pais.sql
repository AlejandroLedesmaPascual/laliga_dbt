{{
  config(
    materialized='view'
  )
}}

WITH base_jugadores AS (
    SELECT
        *
    FROM {{ ref('base_laliga__jugadores') }}
),

base_partidos AS (
    SELECT 
        id_pais AS id,
        pais_competicion AS pais
    FROM {{ ref('base_laliga__partidos') }}
),

stg_pais AS (
    SELECT DISTINCT
        id_pais AS id,
        pais AS pais
    FROM base_jugadores

    UNION

    SELECT DISTINCT
        *
    FROM base_partidos
) SELECT * FROM stg_pais