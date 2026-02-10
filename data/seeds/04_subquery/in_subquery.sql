SELECT id, username FROM t_users WHERE id IN (SELECT DISTINCT user_id FROM t_orders) ORDER BY id
