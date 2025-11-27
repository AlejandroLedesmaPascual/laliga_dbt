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

stg_pierna_dominante AS (
    SELECT DISTINCT
        id_pierna_dominante AS id,
        pierna_dominante   
    FROM base_jugadores
) SELECT * FROM stg_pierna_dominante