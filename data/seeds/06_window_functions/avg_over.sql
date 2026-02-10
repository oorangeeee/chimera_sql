SELECT id, user_id, metric_value, AVG(metric_value) OVER (PARTITION BY metric_name) AS avg_by_metric FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id
