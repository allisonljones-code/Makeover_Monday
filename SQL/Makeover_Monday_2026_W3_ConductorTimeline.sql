-- Data
--CTEs
  WITH conductor_timeline AS (
  SELECT DISTINCT
    show_id,
    person_id,
    show,
    `#_of_performances`,
    production_type,
    production_notes,
    opening_date,
    closing_date,
    first_name,
    last_name,
    person_start_date,
    person_end_date
  FROM `data-projects-478723.makeover_monday.conductor_timeline`),
  
  showrevival AS (
  SELECT
    show,
    COUNT(DISTINCT show_id) AS num_show_ids,
    COUNT(DISTINCT show_id) > 1 AS show_revival_TF
  FROM conductor_timeline
  GROUP BY show
  ),

  revival_rows AS (
    SELECT conductor_timeline.*
    FROM conductor_timeline
    JOIN showrevival
      ON conductor_timeline.show = showrevival.show
    WHERE showrevival.show_revival_TF = TRUE
  ),

  -- collapse multiple runs: one row per person–show title
  person_show_links AS (
    SELECT DISTINCT
      person_id,
      first_name,
      last_name,
      show
    FROM revival_rows
  ),

  -- ranks for stable spacing
  person_nodes AS (
    SELECT
      person_id,
      first_name AS first_name,
      last_name AS last_name,
      DENSE_RANK() OVER (ORDER BY last_name, first_name, person_id) AS person_rank
    FROM person_show_links
    GROUP BY person_id,  first_name, last_name
  ),

  show_nodes AS (
    SELECT
      show,
      DENSE_RANK() OVER (ORDER BY show) AS show_rank
    FROM (SELECT DISTINCT show FROM person_show_links)
  ),

  scaffold AS (
    SELECT t
    FROM UNNEST(GENERATE_ARRAY(0, 1, 0.04)) AS t
  )

SELECT
    unioned_data.datatype, ##in both tables
    unioned_data.show, ##in both tables
    unioned_data.person_id, ##in both tables
    unioned_data.show_revival_TF,  ##in both tables, only TRUE in "paths" datatype
    unioned_data.show_id,
    unioned_data.year,
    unioned_data.value,
    unioned_data.`#_of_performances`,
    unioned_data.production_type,
    unioned_data.production_notes,
    unioned_data.opening_date,
    unioned_data.closing_date,
    unioned_data.first_name,
    unioned_data.last_name,
    unioned_data.person_start_date,
    unioned_data.person_end_date,
    unioned_data.person_rank,
    unioned_data.show_rank,
    unioned_data.t
FROM ( --UNIONED data 
-- timeline data shape
  SELECT
    "timeline" AS datatype,
    data_long.show_id,
    data_long.person_id,
    data_long.year,
    data_long.value, ##dont really need this
    conductor_timeline.show,
    conductor_timeline.`#_of_performances`,
    conductor_timeline.production_type,
    conductor_timeline.production_notes,
    conductor_timeline.opening_date,
    conductor_timeline.closing_date,
    conductor_timeline.first_name,
    conductor_timeline.last_name,
    conductor_timeline.person_start_date,
    conductor_timeline.person_end_date,
    showrevival.show_revival_TF,
    -- dummy fields for paths
    NULL AS person_rank,
    NULL AS show_rank,
    NULL AS t
    
  FROM `data-projects-478723.makeover_monday.conductor_timeline_data_long` data_long
    LEFT JOIN conductor_timeline 
      ON data_long.show_id = conductor_timeline.show_id 
      AND data_long.person_id = conductor_timeline.person_id
  LEFT JOIN showrevival
    ON conductor_timeline.show = showrevival.show
  WHERE data_long.value = 1

UNION ALL

-- paths datashape
  SELECT
    "paths" AS dataype,
    "" AS show_id,
    l.person_id, ##in both datasets
    NULL AS year,
    NULL AS value,
    l.show,
    NULL as `#_of_performances`,
    "" AS production_notes,
    "" AS production_type,
    NULL AS opening_date,
    NULL AS closing_date,
    l.first_name,
    l.last_name,
    NULL AS person_start_date,
    NULL AS person_end_date,
    TRUE AS show_revival_TF,
    pn.person_rank,
    sn.show_rank,
    s.t
    
  FROM person_show_links l
  JOIN person_nodes pn USING (person_id)
  JOIN show_nodes sn USING (show)
  CROSS JOIN scaffold s

) unioned_data;