WITH raw_data AS (
  SELECT
    df.date,
    type,
    value_kg,
    LAG(value_kg) OVER (PARTITION BY type ORDER BY date) AS previous_day_value
FROM `data-projects-478723.makeover_monday.gold_buillonvault` df
),
prev_value AS (
  SELECT
    date,
    type,
    value_kg,
    previous_day_value,
    ROUND(
      IFNULL(((value_kg - previous_day_value)/previous_day_value)*100,0),
      2) AS percent_difference
  FROM raw_data
 ),
run_id AS (
  SELECT
  *,
  SIGN(percent_difference) AS pos_neg_percent_different,
  LAG(SIGN(percent_difference)) OVER (PARTITION BY type ORDER BY date) AS previous_sign,
  (CASE # a new run starts if the sign changes or if it's the first non-zero entry
    WHEN SIGN(percent_difference) != LAG(SIGN(percent_difference)) OVER (PARTITION BY type ORDER BY date)
    THEN 1
    ELSE 0
  END) AS is_new_run_start
  FROM prev_value
  WHERE percent_difference != 0 #filter out days where the difference is exactly zero, as these can complicate continuity
), 
group_run AS ( #in group run the run_group_id increments every time is_new_run_start is 1 (these will be the jumps)
  SELECT
    *,
    SUM(is_new_run_start) OVER (PARTITION BY type ORDER BY date) AS run_group_id
  FROM run_id
),
run_rank AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY type, run_group_id ORDER BY date) AS rank_asc,
    -- ROW_NUMBER() OVER (PARTITION BY type, run_group_id ORDER BY date DESC) AS rank_desc
  FROM group_run
),
run_summary AS (
  SELECT DISTINCT
    type,
    run_group_id AS jump_id,
    pos_neg_percent_different AS jump_type,

    MIN(date) OVER (PARTITION BY type, run_group_id) AS start_date,
    MAX(date) OVER (PARTITION BY type, run_group_id) AS end_date,
    MAX(value_kg) OVER (PARTITION BY type, run_group_id) AS run_max_value
  FROM run_rank
),
jump_nodes AS (
  SELECT
    type,
    pos_neg_percent_different,
    run_group_id,
    MIN(date) AS start_node_date,
    MAX(date) AS end_node_date,
    COUNT(*) AS duration_days
  FROM run_rank
  GROUP BY
    type,
    pos_neg_percent_different,
    run_group_id
  ORDER BY
    type,
    start_node_date
),
df_1 AS (
  SELECT
    df.date,
    df.type,
    df.value_kg,
    df.previous_day_value,
    df.percent_difference,
    df.pos_neg_percent_different AS jump_type,
    df.run_group_id AS jump_id,
    df.rank_asc AS jump_length,
    jn_start.start_node_date,
    jn_end.end_node_date,
    COALESCE(jn_start.start_node_date,jn_end.end_node_date) AS jump_date, 
    
  FROM run_rank df
    LEFT JOIN jump_nodes AS jn_start 
      ON df.type = jn_start.type AND df.date = jn_start.start_node_date
    LEFT JOIN jump_nodes AS jn_end 
      ON df.type = jn_end.type AND df.date = jn_end.end_node_date      
    ORDER BY 
      type, date
),
scaffold AS ( # standard array generation for paths...
  -- SELECT t
  -- FROM UNNEST(GENERATE_ARRAY(0, 1, 0.04)) AS t
  SELECT step/25.0 AS t #integer step best for date ordering
  FROM UNNEST(GENERATE_ARRAY(0, 25)) AS step 
)

--- END OF CTES
SELECT
  *
FROM ( 
SELECT
  "value_data" AS datatype,
  df_1.date,
  df_1.type,
  df_1.value_kg,
  df_1.previous_day_value,
  df_1.percent_difference,
  df_1.jump_type,
  df_1.jump_id, #rank
  df_1.jump_length,
  df_1.jump_date, 
  NULL AS t,
  NULL AS jump_y
FROM df_1 

UNION ALL

-- paths datashape
SELECT
  "paths" AS datatype,
  -- IMPORTANT: this is the date that spans the run
  DATE_ADD(
    run_summary.start_date,
    INTERVAL CAST(ROUND(run_summary.t * DATE_DIFF(run_summary.end_date, run_summary.start_date, DAY)) AS INT64) DAY
  ) AS date,
  run_summary.type,
  NULL AS value_kg,
  NULL AS previous_day_value,
  NULL AS percent_difference,
  run_summary.jump_type,
  run_summary.jump_id,
  NULL AS jump_length,
  run_summary.start_date AS jump_date,   #or keep as start_date/end_date if you want both
  run_summary.t,
  (run_summary.jump_type * run_summary.run_max_value)* (1 - POW(2 * run_summary.t - 1, 2)) AS jump_y # height is max value in run

FROM (
    SELECT
      run_summary.*,
      s.t
    FROM run_summary
    CROSS JOIN scaffold s
  ) run_summary
) 
-- WHERE jump_id = 8 AND type = "close_kg"




