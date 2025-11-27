{{
  config(
    materialized='view'
  )
}}

WITH source_partidos AS (
    SELECT 
        * 
    FROM {{ source('laliga', 'partidos') }}
),

base_partidos AS (
    SELECT
        CAST(temporada AS VARCHAR(20)) AS temporada,
        {{ dbt_utils.generate_surrogate_key(
                                ['temporada']) }} AS id_temporada,
        CAST(jornada AS VARCHAR(20)) AS jornada,
        {{ dbt_utils.generate_surrogate_key(
                                ['jornada',
                                 'temporada']) }} AS id_jornada,
        CAST(competicion AS VARCHAR(20)) AS competicion,
        {{ dbt_utils.generate_surrogate_key(
                                ['competicion']) }} AS id_competicion,
        CAST(pais_competicion AS VARCHAR(50)) AS pais_competicion,
        {{ dbt_utils.generate_surrogate_key(
                                ['pais_competicion']) }} AS id_pais,
        id_partido,
        CAST(arbitro AS VARCHAR(50)) AS arbitro,
        {{ dbt_utils.generate_surrogate_key(
                                ['arbitro']) }} AS id_arbitro,
        CAST(equipo_local AS VARCHAR(50)) AS equipo_local,
        {{ dbt_utils.generate_surrogate_key(
                                ['equipo_local']) }} AS id_equipo_local,
        CAST(equipo_visitante AS VARCHAR(50)) AS equipo_visitante,
        {{ dbt_utils.generate_surrogate_key(
                                ['equipo_visitante']) }} AS id_equipo_visitante,
        CAST(resultado_local AS INT) AS resultado_local,
        CAST(resultado_visitante AS INT) AS resultado_visitante,
        CAST(formacion_local AS VARCHAR(50)) AS formacion_local,
        {{ dbt_utils.generate_surrogate_key(
                                ['formacion_local']) }} AS id_formacion_local,
        CAST(formacion_visitante AS VARCHAR(50)) AS formacion_visitante,
        {{ dbt_utils.generate_surrogate_key(
                                ['formacion_visitante']) }} AS id_formacion_visitante,
        CAST(estadio AS VARCHAR(50)) AS estadio,
        {{ dbt_utils.generate_surrogate_key(
                                ['estadio','aforo']) }} AS id_estadio,
        CAST(aforo AS INT) AS aforo,
        CAST(asistencia AS INT) AS num_asistentes,
        {{ cambiar_formato_fecha('CAST(fecha AS TIMESTAMP_TZ)') }} AS fecha_partido_utc,
        {{ cambiar_formato_fecha('fecha_carga') }} AS fecha_carga_utc
    FROM source_partidos
) SELECT * FROM base_partidos