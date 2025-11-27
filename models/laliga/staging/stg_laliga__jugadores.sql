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

stg_jugadores AS (
    SELECT DISTINCT
        id_jugador AS id,
        nombre_completo,
        nombre,
        id_pierna_dominante,
        id_posicion,
        id_pais,
        altura_cm,
        peso_kg,
        elo,
        potencial,
        fecha_nacimiento
    FROM base_jugadores
) SELECT * FROM stg_jugadores