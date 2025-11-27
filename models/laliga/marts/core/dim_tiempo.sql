{{ config(
    materialized='table'
) }}

WITH minute_spine AS (
    -- 1. Generamos un "esqueleto" de un solo día ficticio
    -- Usamos dbt_utils para generar 1440 filas (60 mins * 24 horas)
    {{ dbt_utils.date_spine(
        datepart="minute",
        start_date="cast('2000-01-01 00:00:00' as timestamp)",
        end_date="cast('2000-01-02 00:00:00' as timestamp)"
    ) }}
),

final AS (
    SELECT
        -- CLAVE PRIMARIA (Integer)
        -- Generamos un ID tipo HHMM (Ej: 2359, 130, 0)
        (EXTRACT(HOUR FROM date_minute) * 100) + EXTRACT(MINUTE FROM date_minute) as id,

        -- FORMATOS DE TIEMPO
        TO_CHAR(date_minute, 'HH24:MI') as hora_texto_24h,   -- "14:05"
        TO_CHAR(date_minute, 'hh12:MI AM') as hora_texto_12h, -- "02:05 PM"
        
        -- COMPONENTES NUMÉRICOS
        EXTRACT(HOUR FROM date_minute) as hora_24,
        EXTRACT(MINUTE FROM date_minute) as minuto,
        
        -- AGREGRACIONES ÚTILES
        CASE 
            WHEN EXTRACT(HOUR FROM date_minute) < 12 THEN 'AM'
            ELSE 'PM'
        END as periodo_am_pm,

        -- FRANJAS HORARIAS (Ejemplo para TV / Audiencias)
        CASE
            WHEN EXTRACT(HOUR FROM date_minute) BETWEEN 0 AND 5 THEN 'Madrugada'
            WHEN EXTRACT(HOUR FROM date_minute) BETWEEN 6 AND 12 THEN 'Mañana'
            WHEN EXTRACT(HOUR FROM date_minute) BETWEEN 13 AND 16 THEN 'Sobremesa'
            WHEN EXTRACT(HOUR FROM date_minute) BETWEEN 17 AND 20 THEN 'Tarde'
            WHEN EXTRACT(HOUR FROM date_minute) BETWEEN 21 AND 23 THEN 'Prime Time'
            ELSE 'Desconocido'
        END as franja_horario

    FROM minute_spine
)

SELECT * FROM final
ORDER BY id