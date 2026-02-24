{{ config(
    materialized = 'table',
    persist_docs = {
      "relation": true,
      "columns": true
    }
) }}


WITH base AS (
  SELECT
    MEGYE_NEVE,
    NEM_MEGNEV,
    COUNT(*) AS cnt,
    COUNT(VIMA_ID) as _COUNT 
  FROM {{ ref('silver_db2107') }}
  GROUP BY MEGYE_NEVE, NEM_MEGNEV
),

enriched AS (
  SELECT
    MEGYE_NEVE,
    NEM_MEGNEV,
    cnt,
    _COUNT,

    -- megyei összes
    SUM(cnt) OVER (
      PARTITION BY MEGYE_NEVE
    ) AS megye_total,

    -- országos összes
    SUM(cnt) OVER () AS orszagos_total,

    -- nemenkénti országos összes
    SUM(cnt) OVER (
      PARTITION BY NEM_MEGNEV
    ) AS orszagos_nem_total
  FROM base
)
SELECT
  MEGYE_NEVE,
  NEM_MEGNEV,
  _COUNT,

  -- megyei arány
  cnt * 1.0 / megye_total AS megyei_arany,

  -- országos arány (nemek megoszlása országosan)
  orszagos_nem_total * 1.0 / orszagos_total AS orszagos_arany,
  megyei_arany - orszagos_arany as diff_arany
FROM enriched
ORDER BY MEGYE_NEVE, NEM_MEGNEV;