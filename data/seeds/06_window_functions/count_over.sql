SELECT id, user_id, status, COUNT(*) OVER (PARTITION BY status) AS status_count FROM t_orders ORDER BY status, id
