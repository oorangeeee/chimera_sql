SELECT id, user_id, metric_name, metric_value, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id) AS rn FROM t_metrics ORDER BY user_id, id
