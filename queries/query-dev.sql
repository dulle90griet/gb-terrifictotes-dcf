SELECT sales_order_id
FROM fact_sales_order
GROUP BY sales_order_id
HAVING COUNT(*) > 1;

-- SELECT *
-- FROM fact_sales_order
-- WHERE sales_order_id IN (11264, 11263, 11387, 11388)
-- ORDER BY sales_order_id, sales_record_id;

SELECT COUNT(DISTINCT sales_record_id) FROM fact_sales_order;

SELECT COUNT(DISTINCT sales_order_id) FROM fact_sales_order;


-- ##########################################################
-- ######  GET TOP 3 DESIGNS   ##############################
-- ######  FOR EACH OF THE TOP 5 TERRITORIES  ###############
-- ######  BY UNITS SOLD  ###################################
-- ##########################################################

-- WITH most_recent_records AS (
--   WITH records_by_id AS (
--     SELECT *,
--       RANK() OVER(
--         PARTITION BY sales_order_id
--         ORDER BY last_updated_date DESC,
--           last_updated_time DESC
--       ) AS r
--     FROM fact_sales_order
--   )
--   SELECT *
--   FROM records_by_id
--   WHERE r = 1
-- )
-- SELECT top_five_territories.rank,
--   top_five_territories.country,
--   dim_design.design_id,
--   dim_design.design_name AS design,
--   top_three_designs_by_country.chart_pos,
--   top_three_designs_by_country.total_sales
-- FROM (
--   SELECT loc.country,
--     RANK() OVER(
--       ORDER BY SUM(new.units_sold) DESC
--     ) AS rank
--   FROM most_recent_records AS new
--   JOIN dim_location AS loc
--     ON new.agreed_delivery_location_id = loc.location_id
--   GROUP BY loc.country
--   ORDER BY SUM(new.units_sold) DESC
--   LIMIT 5
-- ) AS top_five_territories
-- JOIN (
--   SELECT *
--   FROM (
--     SELECT loc.country,
--       new.design_id,
--       SUM(new.units_sold) AS total_sales,
--       RANK() OVER(
--         PARTITION BY loc.country
--         ORDER BY SUM(new.units_sold) DESC
--       ) AS chart_pos
--     FROM most_recent_records AS new
--     JOIN dim_location AS loc
--       ON new.agreed_delivery_location_id = loc.location_id
--     GROUP BY new.design_id, loc.country
--     ORDER BY loc.country, total_sales DESC
--   ) AS design_sales_by_country
--   WHERE chart_pos < 4
-- ) AS top_three_designs_by_country
--   ON top_five_territories.country = top_three_designs_by_country.country
-- JOIN dim_design
--   ON top_three_designs_by_country.design_id = dim_design.design_id
-- ORDER BY top_five_territories.rank, top_three_designs_by_country.chart_pos;


-- ##########################################################
-- ######  CHECK FOR GAPS IN DIM_DESIGN   ###################

SELECT design_id + 1 as design_gap_starts,
  next_design_id - 1 as design_gap_ends
FROM (
  SELECT design_id,
    LEAD(design_id, 1, design_id) OVER(
      ORDER BY design_id
    ) AS next_design_id
  FROM dim_design
) AS current_and_next
WHERE next_design_id - design_id > 1;


-- ##########################################################
-- ######  CHECK FOR GAPS IN DIM_COUNTERPARTY   #############

SELECT counterparty_id + 1 as counterparty_gap_starts,
  next_counterparty_id - 1 as counterparty_gap_ends
FROM (
  SELECT counterparty_id,
    LEAD(counterparty_id, 1, counterparty_id) OVER(
      ORDER BY counterparty_id
    ) AS next_counterparty_id
  FROM dim_counterparty
) AS current_and_next
WHERE next_counterparty_id - counterparty_id > 1;


-- ##########################################################
-- ######  CHECK FOR GAPS IN DIM_CURRENCY  ##################

SELECT currency_id + 1 as currency_gap_starts,
  next_currency_id - 1 as currency_gap_ends
FROM (
  SELECT currency_id,
    LEAD(currency_id, 1, currency_id) OVER(
      ORDER BY currency_id
    ) AS next_currency_id
  FROM dim_currency
) AS current_and_next
WHERE next_currency_id - currency_id > 1;


-- ##########################################################
-- ######  CHECK FOR GAPS IN DIM_LOCATION  ##################

SELECT location_id + 1 as location_gap_starts,
  next_location_id - 1 as location_gap_ends
FROM (
  SELECT location_id,
    LEAD(location_id, 1, location_id) OVER(
      ORDER BY location_id
    ) AS next_location_id
  FROM dim_location
) AS current_and_next
WHERE next_location_id - location_id > 1;


-- ##########################################################
-- ######  CHECK FOR GAPS IN DIM_STAFF  #####################

SELECT staff_id + 1 as staff_gap_starts,
  next_staff_id - 1 as staff_gap_ends
FROM (
  SELECT staff_id,
    LEAD(staff_id, 1, staff_id) OVER(
      ORDER BY staff_id
    ) AS next_staff_id
  FROM dim_staff
) AS current_and_next
WHERE next_staff_id - staff_id > 1;


-- ##########################################################
-- ######  CHECK FOR GAPS IN FACT_SALES_ORDER  ##############

SELECT sales_order_id + 1 as sales_order_gap_starts,
  next_sales_order_id - 1 as sales_order_gap_ends
FROM (
  SELECT sales_order_id,
    LEAD(sales_order_id, 1, sales_order_id) OVER(
      ORDER BY sales_order_id
    ) AS next_sales_order_id
  FROM fact_sales_order
) AS current_and_next
WHERE next_sales_order_id - sales_order_id > 1;
