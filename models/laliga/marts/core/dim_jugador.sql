{{
  config(
    materialized='table'
  )
}}

WITH stg_jugadores AS (
    SELECT
        *
    FROM {{ ref('snapshot_jugadores_tracking') }}
),

stg_pierna_dominante AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__pierna_dominante') }}
),

stg_pais AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__pais') }}
),

stg_posicion AS (
    SELECT 
        *
    FROM {{ ref('stg_laliga__posicion') }}
),

dim_jugador AS (
    SELECT 
        j.id,
        nombre_completo,
        nombre,
        altura_cm,
        peso_kg,
        elo,
        potencial,
        posicion,
        pierna_dominante,
        pais,
        fecha_nacimiento
    FROM stg_jugadores j
    JOIN stg_pierna_dominante d
    ON j.id_pierna_dominante = d.id
    JOIN stg_posicion p
    ON p.id = j.id_posicion
    JOIN stg_pais pa 
    ON pa.id = j.id_pais
) SELECT * FROM dim_jugador