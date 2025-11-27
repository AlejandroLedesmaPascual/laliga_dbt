{{
  config(
    materialized='view'
  )
}}

WITH source_jugador AS (
    SELECT 
        *
    FROM {{ source('laliga', 'jugadores') }}
),

base_jugador AS (
    SELECT
        CAST(pais AS VARCHAR(50)) AS pais,
        {{ dbt_utils.generate_surrogate_key(
                                ['pais']) }} AS id_pais,
        CAST(fecha_nacimiento AS DATE) AS fecha_nacimiento,
        CAST(elo AS INT) AS elo,
        {{ dbt_utils.generate_surrogate_key(
                                ['pierna_dominante']) }} AS id_pierna_dominante,
        CAST(pierna_dominante AS VARCHAR(25)) AS pierna_dominante,
        CAST(nombre_completo AS VARCHAR(100)) AS nombre_completo,
        CAST(altura AS INT) AS altura_cm,
        {{ dbt_utils.generate_surrogate_key(
                                ['id_jugador']) }} AS id_jugador,
        CAST(nombre AS VARCHAR(50)) AS nombre,
        {{ dbt_utils.generate_surrogate_key(
                                ['posicion']) }} AS id_posicion,
        CAST(posicion AS VARCHAR(50)) AS posicion,
        CAST(potencial AS INT) AS potencial,
        CAST(peso AS INT) AS peso_kg,
        {{ cambiar_formato_fecha('fecha_carga') }} AS fecha_carga_utc
    FROM source_jugador
) SELECT * FROM base_jugador
