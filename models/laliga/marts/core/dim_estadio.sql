{{
  config(
    materialized='table'
  )
}}

WITH stg_estadios AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__estadios') }}
),

dim_estadio AS (
    SELECT 
        id,
        nombre,
        aforo
    FROM stg_estadios
    ORDER BY nombre
) SELECT * FROM dim_estadio