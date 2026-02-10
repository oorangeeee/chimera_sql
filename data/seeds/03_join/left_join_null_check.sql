SELECT u.id, u.username FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id WHERE o.id IS NULL ORDER BY u.id
