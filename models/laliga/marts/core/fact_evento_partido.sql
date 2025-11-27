{{
  config(
    materialized='incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
  )
}}

WITH int_eventos_partido AS (
    SELECT
        *
    FROM {{ ref('int_eventos_partido__joined') }}
    {% if is_incremental() %}
    WHERE fecha_carga_utc > (SELECT max(fecha_carga_utc) FROM {{ this }})
    {% endif %}
), 

fact_eventos AS (
    SELECT
        id,
        id_jugador,
        id_partido,
        id_tipo_evento,
        id_equipo,
        id_estadio,
        id_jornada,
        id_arbitro,
        tipo_evento,
        minuto_evento,
        localidad,
        parte,
        fecha_carga_utc
    FROM int_eventos_partido
) SELECT * FROM fact_eventos