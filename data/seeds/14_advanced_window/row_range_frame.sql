SELECT id, metric_value, AVG(metric_value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id
