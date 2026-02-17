WITH bigmaxindex AS (
  SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,   
    currency_code,
    (CASE
      WHEN name = "UAE" THEN "United Arab Emirates" -- found that duplicate name mismatch is iso_a3
      ELSE name
    END) AS name,
    local_price,
    dollar_ex,
    dollar_price
  FROM `data-projects-478723.makeover_monday.bigmacindex`
),

country_region AS (
  -- https://corporate.mcdonalds.com/content/dam/sites/corp/nfl/pdf/20231202_Investor%20Update%20Fact%20Sheet.pdf
  SELECT * FROM UNNEST([
   
    -- North America
      STRUCT("United States" AS name, "North America"  AS region_group),
      STRUCT("Canada", "North America"),

    -- Western Europe 
      STRUCT("Britain", "Western Europe" ),
      STRUCT("Switzerland", "Western Europe"),
      STRUCT("Norway", "Western Europe"),
      STRUCT("Sweden", "Western Europe"),
      STRUCT("Denmark", "Western Europe"),
      STRUCT("Euro area", "Western Europe"),
      
    -- Eastern Europe
      STRUCT("Czech Republic", "Eastern Europe"),
      STRUCT("Poland", "Eastern Europe"),
      STRUCT("Hungary", "Eastern Europe"),
      STRUCT("Romania", "Eastern Europe"),
      STRUCT("Moldova", "Eastern Europe"),
      STRUCT("Ukraine", "Eastern Europe"),
      STRUCT("Russia", "Eastern Europe"),
      STRUCT("Turkey", "Eastern Europe"),

    -- Asia
      STRUCT("Japan", "Asia"),
      STRUCT("South Korea","Asia"),
      STRUCT("Singapore", "Asia"),
      STRUCT("Taiwan", "Asia"),
      STRUCT("Hong Kong", "Asia"),
    
      STRUCT("China", "Asia"),
      STRUCT("India", "Asia"),
      STRUCT("Indonesia", "Asia"),
      STRUCT("Malaysia", "Asia"),
      STRUCT("Philippines", "Asia"),
      STRUCT("Thailand", "Asia"),
      STRUCT("Vietnam", "Asia"),
      STRUCT("Pakistan", "Asia"),
      STRUCT("Sri Lanka", "Asia"),
      
      STRUCT("Australia", "Asia"),
      STRUCT("New Zealand", "Asia"),


    -- Middle East and Africa
      STRUCT("United Arab Emirates","Middle East and Africa"),
      STRUCT("Kuwait", "Middle East and Africa"),
      STRUCT("Qatar", "Middle East and Africa"),
      STRUCT("Oman", "Middle East and Africa"),
      STRUCT("Saudi Arabia", "Middle East and Africa"),
      STRUCT("Bahrain", "Middle East and Africa"),
      STRUCT("Lebanon", "Middle East and Africa"),
      STRUCT("Jordan", "Middle East and Africa"),
      STRUCT("Israel", "Middle East and Africa"),
      STRUCT("Egypt", "Middle East and Africa"),
      STRUCT("South Africa", "Middle East and Africa"),
      STRUCT("Azerbaijan", "Middle East and Africa"),

    -- Latin America
      STRUCT("Argentina", "Latin America"),
      STRUCT("Brazil", "Latin America"),
      STRUCT("Chile", "Latin America"),
      STRUCT("Colombia", "Latin America"),
      STRUCT("Mexico", "Latin America"),
      STRUCT("Peru", "Latin America"),
      STRUCT("Uruguay", "Latin America"),
      STRUCT("Venezuela", "Latin America"),
      STRUCT("Costa Rica", "Latin America"),
      STRUCT("Guatemala", "Latin America"),
      STRUCT("Honduras", "Latin America"),
      STRUCT("Nicaragua", "Latin America")

  ])
),

usbmi_monthly AS (
  SELECT
    year,
    month,
    currency_code,
    name,
    AVG(dollar_price) AS average_monthly_dollar_price
  FROM bigmaxindex
  WHERE name = "United States" 
  GROUP BY year, month, currency_code, name
)

SELECT
  bmi.date,
  bmi.year,
  bmi.month,
  bmi.currency_code,
  bmi.name,
  COALESCE(cr.region_group, "Unmapped") AS region_group,
  bmi.local_price,
  bmi.dollar_ex,
  bmi.dollar_price, -- this is what we will compare against,
  us_bmi.average_monthly_dollar_price,
  SAFE_DIVIDE(bmi.dollar_price,us_bmi.average_monthly_dollar_price) AS relative_price, -- closer to 1 is the same as the US
  AVG(bmi.dollar_price) OVER (
    PARTITION BY 
    bmi.year, bmi.month, cr.region_group
  ) AS region_avg_dollar_price,
  SAFE_DIVIDE(
    bmi.dollar_price,
    AVG(bmi.dollar_price) OVER (
      PARTITION BY bmi.year, bmi.month, cr.region_group
    )
  ) AS relative_price_region

FROM bigmaxindex bmi
  LEFT JOIN usbmi_monthly us_bmi 
  ON bmi.year = us_bmi.year
  AND bmi.month = us_bmi.month

  LEFT JOIN country_region cr
    ON bmi.name = cr.name
