WITH stg_eventos_partido AS (
    SELECT
        *
    FROM {{ ref('stg_laliga__eventos_partido') }}    
),

stg_tipo_eventos AS (
    SELECT 
        *
    FROM {{ ref('stg_laliga__tipo_eventos') }} 
),

stg_partidos AS (
    SELECT 
        *
    FROM {{ ref('stg_laliga__partidos') }}
),

goles_calculados AS (
    SELECT
        e.id_partido,
        SUM(CASE 
            WHEN tipo_evento IN ('Goal', 'Goal from penalty', 'Free-kick goal', 'Own goal') AND localidad = 'home'
            THEN 1 ELSE 0 
        END) as resultado_local_calculado,
        
        SUM(CASE 
            WHEN tipo_evento IN ('Goal', 'Goal from penalty', 'Free-kick goal', 'Own goal') AND localidad = 'away'
            THEN 1 ELSE 0 
        END) as resultado_visitante_calculado
    FROM stg_eventos_partido e
    JOIN stg_laliga__tipo_eventos t 
    ON e.id_tipo_evento = t.id
    JOIN stg_partidos p
    ON e.id_partido = p.id
    GROUP BY e.id_partido
),

marcador AS (
    SELECT
        id,
        resultado_local,
        resultado_visitante,
        fecha_partido_utc
    FROM stg_partidos

),

comprobacion AS (
    SELECT
        m.id,
        fecha_partido_utc,
        resultado_local,
        resultado_local_calculado,
        resultado_visitante,
        resultado_visitante_calculado
    FROM marcador m
    JOIN goles_calculados c
    ON m.id = c.id_partido
    WHERE resultado_local != resultado_local_calculado
    OR resultado_visitante != resultado_visitante_calculado
) SELECT * FROM comprobacion