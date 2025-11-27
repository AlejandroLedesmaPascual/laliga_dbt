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

stg_partidos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(
                                ['jornada','temporada','competicion','equipo_local','equipo_visitante']) }} AS id,
        id_jornada,
        id_equipo_local,
        id_equipo_visitante,
        id_arbitro,
        id_formacion_local,
        id_formacion_visitante,
        id_competicion,
        id_estadio,
        resultado_local,
        resultado_visitante,
        num_asistentes,
        fecha_partido_utc
    FROM base_partidos
) SELECT * FROM stg_partidos