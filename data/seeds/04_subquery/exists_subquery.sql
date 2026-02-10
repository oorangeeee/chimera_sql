SELECT id, username FROM t_users u WHERE EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id AND o.status = 'delivered') ORDER BY id
