SELECT u.id, u.username, COUNT(o.id) AS order_count, SUM(o.total_price) AS total_spent FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username ORDER BY u.id
