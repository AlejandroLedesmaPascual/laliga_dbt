WITH jugadores_unicos_partido AS (
    SELECT DISTINCT 
        id_partido,
        id_jugador,
        localidad
    FROM stg_laliga__eventos_partido
),

calculo_medias AS (
    SELECT 
        jp.id_partido,
        AVG(CASE 
            WHEN jp.localidad = 'home' THEN j.elo 
            ELSE NULL 
        END) AS media_elo_local,

        AVG(CASE 
            WHEN jp.localidad = 'away' THEN j.elo 
            ELSE NULL 
        END) AS media_elo_visitante
    FROM jugadores_unicos_partido jp
    JOIN stg_laliga__jugadores j 
    ON jp.id_jugador = j.id
    GROUP BY jp.id_partido
) SELECT * FROM calculo_medias