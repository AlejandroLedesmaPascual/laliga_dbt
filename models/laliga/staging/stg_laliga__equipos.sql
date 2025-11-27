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
        id_equipo_local AS id,
        equipo_local AS nombre
    FROM base_partidos
) SELECT * FROM stg_equipos