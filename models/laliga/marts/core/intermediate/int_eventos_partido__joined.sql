{{
  config(
    materialized='incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
  )
}}

WITH stg_eventos_partido AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__eventos_partido') }}
    {% if is_incremental() %}
    WHERE fecha_carga_utc > (SELECT max(fecha_carga_utc) FROM {{ this }})
    {% endif %}
),

stg_jugadores AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__jugadores') }}
),

stg_equipos AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__equipos') }}    
),

stg_partidos AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__partidos') }} 
),

stg_tipo_eventos AS (
    SELECT 
        *
    FROM {{ ref('stg_laliga__tipo_eventos') }}
),

int_eventos_partido AS (
    SELECT 
        e.id,
        e.id_jugador,
        e.id_partido,
        e.id_tipo_evento,
        CASE 
            WHEN localidad = 'home' THEN p.id_equipo_local
            WHEN localidad = 'away' THEN p.id_equipo_visitante
        END AS id_equipo,
        p.id_estadio,
        p.id_jornada,
        p.fecha_partido_utc,
        p.id_arbitro,
        t.tipo_evento,
        minuto_evento,
        localidad,
        CASE 
            WHEN minuto_evento <= 45 THEN 'Primera Parte'
            WHEN minuto_evento > 45 THEN 'Segunda Parte'
        END AS parte,
        fecha_carga_utc
    FROM stg_eventos_partido e
    JOIN stg_partidos p
    ON p.id = e.id_partido
    JOIN stg_tipo_eventos t
    ON e.id_tipo_evento = t.id
) SELECT * FROM int_eventos_partido