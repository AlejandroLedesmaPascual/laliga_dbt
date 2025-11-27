{% snapshot snapshot_jugadores_tracking %}

{{
    config(
      unique_key='id',
      strategy='check',
      check_cols=['id_posicion', 'id_pais']
    )
}}

SELECT 
    *
FROM {{ ref('stg_laliga__jugadores') }}

{% endsnapshot %}