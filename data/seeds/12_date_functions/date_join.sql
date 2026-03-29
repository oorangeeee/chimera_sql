SELECT u.id, u.username, o.id AS order_id, o.order_date FROM t_users u INNER JOIN t_orders o ON u.id = o.user_id WHERE o.order_date IS NOT NULL ORDER BY o.order_date, o.id
