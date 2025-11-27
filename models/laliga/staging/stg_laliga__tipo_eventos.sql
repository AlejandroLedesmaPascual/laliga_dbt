{{
  config(
    materialized='view'
  )
}}

WITH base_eventos_partido AS (
    SELECT
        *
    FROM {{ ref('base_laliga__eventos_partido') }}
),

stg_tipo_eventos AS (
    SELECT DISTINCT
        id_tipo_evento AS id,
        tipo_evento
    FROM base_eventos_partido  
) SELECT * FROM stg_tipo_eventos