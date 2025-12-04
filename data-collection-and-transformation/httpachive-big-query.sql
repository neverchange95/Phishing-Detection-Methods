--Queries httparchive big query with 50/50 Root/Non-Root URLs + 30% http

DECLARE target_date DATE   DEFAULT '2025-10-01';   -- partition
DECLARE client      STRING DEFAULT 'desktop';      -- 'desktop' or 'mobile'
DECLARE top_rank    INT64  DEFAULT 1000000;        -- CrUX Top 1 Million
DECLARE target_rows INT64  DEFAULT 90000;

-- Upper limits per Origin (against dominance of individual hosts)
DECLARE max_per_origin_non_root INT64 DEFAULT 5;
DECLARE max_per_origin_root     INT64 DEFAULT 5;
DECLARE max_per_origin_http     INT64 DEFAULT 2;

-- 50/50 root vs. non-root, http total 30%
DECLARE q_root_total      INT64 DEFAULT CAST(FLOOR(target_rows / 2) AS INT64);
DECLARE q_non_root_total  INT64 DEFAULT target_rows - q_root_total;

DECLARE q_http_total      INT64 DEFAULT CAST(ROUND(target_rows * 0.30) AS INT64);
DECLARE q_http_root       INT64 DEFAULT CAST(FLOOR(q_http_total / 2) AS INT64);
DECLARE q_http_non_root   INT64 DEFAULT q_http_total - q_http_root;

DECLARE q_https_root      INT64 DEFAULT q_root_total     - q_http_root;
DECLARE q_https_non_root  INT64 DEFAULT q_non_root_total - q_http_non_root;

WITH pages_base AS (
  SELECT
    p.page AS url,
    p.date AS discovered_time_date,
    p.rank,
    p.is_root_page,
    `httparchive.fn.GET_ORIGIN`(p.page) AS origin,
    CASE
      WHEN STARTS_WITH(p.page, 'http://')  THEN 'http'
      WHEN STARTS_WITH(p.page, 'https://') THEN 'https'
      ELSE 'other'
    END AS scheme
  FROM `httparchive.crawl.pages` AS p
  WHERE
    p.date = target_date
    AND p.client = client
    AND p.rank IS NOT NULL AND p.rank <= top_rank
),

-- ---------- http, Non-Root ----------
http_non_root_stage AS (
  SELECT
    url, discovered_time_date, rank, origin,
    ROW_NUMBER() OVER (
      PARTITION BY origin
      ORDER BY rank, FARM_FINGERPRINT(url)
    ) AS rn_per_origin
  FROM pages_base
  WHERE scheme = 'http' AND is_root_page = FALSE
),
http_non_root AS (
  SELECT url, discovered_time_date, rank
  FROM (
    SELECT
      url, discovered_time_date, rank,
      ROW_NUMBER() OVER (ORDER BY rank, FARM_FINGERPRINT(url)) AS rn_global
    FROM http_non_root_stage
    WHERE rn_per_origin <= max_per_origin_http
  )
  WHERE rn_global <= q_http_non_root
),

-- ---------- http, Root ----------
http_root_stage AS (
  SELECT
    url, discovered_time_date, rank, origin,
    ROW_NUMBER() OVER (
      PARTITION BY origin
      ORDER BY rank, FARM_FINGERPRINT(url)
    ) AS rn_per_origin
  FROM pages_base
  WHERE scheme = 'http' AND is_root_page = TRUE
),
http_root AS (
  SELECT url, discovered_time_date, rank
  FROM (
    SELECT
      url, discovered_time_date, rank,
      ROW_NUMBER() OVER (ORDER BY rank, FARM_FINGERPRINT(url)) AS rn_global
    FROM http_root_stage
    WHERE rn_per_origin <= max_per_origin_http
  )
  WHERE rn_global <= q_http_root
),

-- ---------- https, Non-Root ----------
https_non_root_stage AS (
  SELECT
    url, discovered_time_date, rank, origin,
    ROW_NUMBER() OVER (
      PARTITION BY origin
      ORDER BY rank, FARM_FINGERPRINT(url)
    ) AS rn_per_origin
  FROM pages_base
  WHERE scheme = 'https' AND is_root_page = FALSE
),
https_non_root AS (
  SELECT url, discovered_time_date, rank
  FROM (
    SELECT
      url, discovered_time_date, rank,
      ROW_NUMBER() OVER (ORDER BY rank, FARM_FINGERPRINT(url)) AS rn_global
    FROM https_non_root_stage
    WHERE rn_per_origin <= max_per_origin_non_root
  )
  WHERE rn_global <= q_https_non_root
),

-- ---------- https, Root ----------
https_root_stage AS (
  SELECT
    url, discovered_time_date, rank, origin,
    ROW_NUMBER() OVER (
      PARTITION BY origin
      ORDER BY rank, FARM_FINGERPRINT(url)
    ) AS rn_per_origin
  FROM pages_base
  WHERE scheme = 'https' AND is_root_page = TRUE
),
https_root AS (
  SELECT url, discovered_time_date, rank
  FROM (
    SELECT
      url, discovered_time_date, rank,
      ROW_NUMBER() OVER (ORDER BY rank, FARM_FINGERPRINT(url)) AS rn_global
    FROM https_root_stage
    WHERE rn_per_origin <= max_per_origin_root
  )
  WHERE rn_global <= q_https_root
),

seed_union AS (
  SELECT * FROM http_non_root
  UNION ALL
  SELECT * FROM http_root
  UNION ALL
  SELECT * FROM https_non_root
  UNION ALL
  SELECT * FROM https_root
),
seed_distinct AS (
  SELECT url, discovered_time_date, rank
  FROM (
    SELECT
      url, discovered_time_date, rank,
      ROW_NUMBER() OVER (PARTITION BY url ORDER BY rank) AS rn
    FROM seed_union
  )
  WHERE rn = 1
),

backfill AS (
  SELECT b.url, b.discovered_time_date, b.rank
  FROM pages_base b
  LEFT JOIN seed_distinct s
    ON b.url = s.url
  WHERE s.url IS NULL
  QUALIFY ROW_NUMBER() OVER (ORDER BY b.rank, FARM_FINGERPRINT(b.url))
          <= GREATEST(0, target_rows - (SELECT COUNT(*) FROM seed_distinct))
),

final_pool AS (
  SELECT * FROM seed_distinct
  UNION ALL
  SELECT * FROM backfill
)

SELECT
  url,
  FORMAT_TIMESTAMP(
    '%d/%m/%y %H:%M:%S',
    TIMESTAMP(DATETIME(discovered_time_date, TIME '00:00:00'), 'Europe/Berlin'),
    'Europe/Berlin'
  ) AS discovered_time,
  rank,
  FORMAT_TIMESTAMP('%d/%m/%y %H:%M:%S', CURRENT_TIMESTAMP(), 'Europe/Berlin') AS pulled_time
FROM final_pool
LIMIT 27439; -- Limited results to obtain an approximately 50/50 data set
