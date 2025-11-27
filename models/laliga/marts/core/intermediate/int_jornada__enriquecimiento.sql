{{
  config(
    materialized='table'
  )
}}

WITH stg_jornada AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__jornada') }}
),

int_jornada AS (
    SELECT
        *,
        CASE 
            WHEN TRY_TO_NUMBER(REGEXP_REPLACE(jornada, '[^0-9]', '')) <= 19 THEN 'Primera Vuelta'
            WHEN TRY_TO_NUMBER(REGEXP_REPLACE(jornada, '[^0-9]', '')) > 19 THEN 'Segunda Vuelta'
            ELSE 'Eliminatoria'
        END AS vuelta
    FROM stg_jornada
) SELECT * FROM int_jornada