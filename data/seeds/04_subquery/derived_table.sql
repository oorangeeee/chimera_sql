SELECT sub.user_id, sub.order_count, u.username FROM (SELECT user_id, COUNT(*) AS order_count FROM t_orders GROUP BY user_id) sub INNER JOIN t_users u ON sub.user_id = u.id ORDER BY sub.user_id
