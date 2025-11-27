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

stg_formaciones AS (
    SELECT DISTINCT
        id_formacion_local AS id,
        formacion_local AS formacion
    FROM base_partidos
    WHERE formacion != ''

    UNION

    SELECT DISTINCT
        id_formacion_visitante AS id,
        formacion_visitante AS formacion
    FROM base_partidos
    WHERE formacion != ''
) SELECT * FROM stg_formaciones