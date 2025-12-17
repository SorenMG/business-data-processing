CREATE OR REPLACE VIEW trip_enriched AS
SELECT
    t.trip_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    td.pickup_date,
    td.pickup_year,
    td.pickup_month,
    td.pickup_week,
    td.pickup_weekday,
    td.is_weekend,
    EXTRACT(HOUR FROM t.pickup_datetime) AS pickup_hour,
    z.zone_id        AS pu_zone_id,
    z.zone_name      AS pu_zone_name,
    z.borough        AS pu_borough,
    z.is_airport     AS pu_is_airport,
    z.is_manhattan   AS pu_is_manhattan,
    t.passenger_count,
    t.trip_distance_mi,
    t.trip_duration_min,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    (t.total_amount / NULLIF(t.trip_duration_min, 0)) AS revenue_per_minute,
    (t.total_amount / NULLIF(t.trip_distance_mi, 0))  AS revenue_per_mile
FROM trip t
JOIN time_dim td ON t.date_id = td.date_id
JOIN zone z      ON t.pu_zone_id = z.zone_id;

CREATE OR REPLACE VIEW zone_hourly_revenue AS
SELECT
    pu_borough,
    pu_zone_name,
    pickup_weekday,
    is_weekend,
    pickup_hour,
    COUNT(*)                         AS trip_count,
    SUM(total_amount)                AS total_revenue,
    AVG(total_amount)                AS avg_revenue_per_trip,
    AVG(revenue_per_minute)          AS avg_revenue_per_minute,
    AVG(revenue_per_mile)            AS avg_revenue_per_mile
FROM trip_enriched
GROUP BY
    pu_borough, pu_zone_name,
    pickup_weekday, is_weekend,
    pickup_hour;