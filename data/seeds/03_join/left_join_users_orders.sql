SELECT u.id, u.username, o.id AS order_id, o.total_price FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id ORDER BY u.id, o.id
