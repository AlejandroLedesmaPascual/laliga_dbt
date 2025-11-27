{{
  config(
    materialized='table'
  )
}}

SELECT
    de.nombre AS equipo,
    temporada,
    SUM(CASE 
        WHEN fe.tipo_evento IN ('Goal from penalty', 'Missed penalty', 'Penalty saved') THEN 1 
        ELSE 0 
    END) AS total_penaltis_a_favor,
    SUM(CASE 
        WHEN fe.tipo_evento IN ('Yellow card','2nd yellow card leads to red card') THEN 1 
        ELSE 0 
    END) AS total_amarillas,
    SUM(CASE 
        WHEN fe.tipo_evento IN ('Red card', '2nd yellow card leads to red card') THEN 1 
        ELSE 0 
    END) AS total_rojas,

FROM {{ ref('fact_evento_partido') }} fe
JOIN {{ ref('dim_equipo') }} de
    ON fe.id_equipo = de.id
JOIN {{ ref('dim_calendario_competicion')}} dc
ON dc.id = fe.id_jornada
WHERE de.nombre IN ('Real Madrid', 'Barcelona')
GROUP BY 1, 2
ORDER BY 1