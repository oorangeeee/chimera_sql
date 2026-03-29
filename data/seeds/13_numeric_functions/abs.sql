SELECT id, metric_value, ABS(metric_value) AS abs_val FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id
