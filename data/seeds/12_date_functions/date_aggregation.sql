SELECT measurement_date, COUNT(*) AS cnt, AVG(metric_value) AS avg_val FROM t_metrics WHERE measurement_date IS NOT NULL GROUP BY measurement_date ORDER BY measurement_date
