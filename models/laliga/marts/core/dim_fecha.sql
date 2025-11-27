{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    ) }}
),

calculado AS (
    SELECT
        date_day AS id,
        YEAR(date_day) AS anio,
        MONTH(date_day) AS mes_numero,
        MONTHNAME(date_day) AS mes_nombre,
        DAY(date_day) AS dia_numero,
        QUARTER(date_day) AS trimestre,
        DAYOFWEEK(date_day) AS dia_semana_numero,
        DAYNAME(date_day) AS dia_semana_nombre,
        CASE 
            WHEN DAYOFWEEKISO(date_day) IN (6, 7) THEN TRUE 
            ELSE FALSE 
        END AS es_fin_de_semana,
        TO_CHAR(date_day, 'YYYY-MM') AS anio_mes -- Ej: '2023-11'
    FROM date_spine
)

SELECT * FROM calculado