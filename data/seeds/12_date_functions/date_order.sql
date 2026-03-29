SELECT id, measurement_date, metric_value FROM t_metrics WHERE measurement_date IS NOT NULL ORDER BY measurement_date, id
