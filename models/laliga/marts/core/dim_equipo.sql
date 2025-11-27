{{
  config(
    materialized='table'
  )
}}

WITH stg_equipos AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__equipos') }}
),

dim_equipo AS (
    SELECT 
        id,
        nombre 
    FROM stg_equipos
    ORDER BY nombre
) SELECT * FROM dim_equipo