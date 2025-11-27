{{
  config(
    materialized='incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
  )
}}

WITH base_eventos_partido AS (
    SELECT
        *
    FROM {{ ref('base_laliga__eventos_partido') }}
    {% if is_incremental() %}
    WHERE fecha_carga_utc > (SELECT max(fecha_carga_utc) FROM {{ this }})
    {% endif %}
),

base_partidos AS (
    SELECT
        *
    FROM {{ ref('base_laliga__partidos') }}    
),

stg_eventos_partido AS (
    SELECT
        id_jugador,
        {{ dbt_utils.generate_surrogate_key(
                                ['p.jornada',
                                 'p.temporada',
                                 'p.competicion',
                                 'p.equipo_local',
                                 'p.equipo_visitante']) }} AS id_partido,
        id_tipo_evento,
        minuto_evento,
        localidad,
        e.fecha_carga_utc
    FROM base_eventos_partido e
    JOIN base_partidos p
    ON e.id_partido = p.id_partido
),

stg_id_eventos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
                                ['id_jugador',
                                 'id_partido',
                                 'id_tipo_evento',
                                 'minuto_evento']) }} AS id,
        id_jugador,
        id_partido,
        id_tipo_evento,
        minuto_evento,
        localidad,
        fecha_carga_utc
    FROM stg_eventos_partido
) SELECT * FROM stg_id_eventos
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id 
    ORDER BY fecha_carga_utc DESC
  ) = 1
