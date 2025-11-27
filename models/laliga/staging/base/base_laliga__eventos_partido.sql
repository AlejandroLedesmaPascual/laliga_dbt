{{
  config(
    materialized='view'
  )
}}

WITH source_eventos_partidos AS (
    SELECT 
        * 
    FROM {{ source('laliga', 'eventos_partido') }}
),

base_eventos_partido AS (
    SELECT
        id_partido,
        CAST(minuto_evento AS INT) AS minuto_evento,
        {{ dbt_utils.generate_surrogate_key(
                                ['jugador']) }} AS id_jugador,
        CAST(es_local AS VARCHAR(10)) AS localidad,
        {{ dbt_utils.generate_surrogate_key(
                                ['tipo_evento']) }} AS id_tipo_evento,
        CAST(tipo_evento AS VARCHAR(40)) AS tipo_evento,
        {{ cambiar_formato_fecha('fecha_carga') }} AS fecha_carga_utc
    FROM source_eventos_partidos
) SELECT * FROM base_eventos_partido