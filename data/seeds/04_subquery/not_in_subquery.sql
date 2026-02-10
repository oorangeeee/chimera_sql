SELECT id, username FROM t_users WHERE id NOT IN (SELECT DISTINCT user_id FROM t_orders WHERE user_id IS NOT NULL) ORDER BY id
