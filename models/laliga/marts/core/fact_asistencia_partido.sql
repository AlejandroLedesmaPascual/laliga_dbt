{{
  config(
    materialized='table'
  )
}}

WITH int_media AS (
    SELECT
        *
    FROM {{ ref('int_media_elo_equipo__grouped') }}
),

stg_partidos AS (
    SELECT 
        *
    FROM {{ ref('stg_laliga__partidos' )}}
),

stg_estadios AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__estadios') }}
),

fact_asistencia AS (
    SELECT
        p.id,
        id_equipo_local,
        id_equipo_visitante,
        id_estadio,
        id_jornada AS id_calendario_competicion,
        CAST(TO_CHAR(fecha_partido_utc, 'YYYYMMDD') AS INT) AS id_fecha,
        (EXTRACT(HOUR FROM fecha_partido_utc) * 100) + EXTRACT(MINUTE FROM fecha_partido_utc) AS id_tiempo,
        ROUND((num_asistentes / aforo) * 100,0) AS porcentaje_asistencia,
        num_asistentes,
        ROUND(media_elo_local,0) AS media_elo_local,
        ROUND(media_elo_visitante,0) AS media_elo_visitante
    FROM stg_partidos p
    JOIN stg_estadios es
    ON p.id_estadio = es.id
    JOIN int_media m
    ON m.id_partido = p.id
) SELECT * FROM fact_asistencia