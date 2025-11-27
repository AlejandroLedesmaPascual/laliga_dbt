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

stg_posicion AS (
    SELECT DISTINCT
        id_posicion AS id,
        REGEXP_SUBSTR(
        posicion,
        '\\((.*?)\\)',
        1,
        1,
        'e',
        1
    ) AS posicion  
    FROM base_jugadores
) SELECT * FROM stg_posicion