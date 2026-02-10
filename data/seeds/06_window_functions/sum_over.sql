SELECT id, user_id, total_price, SUM(total_price) OVER (PARTITION BY user_id ORDER BY id) AS running_total FROM t_orders ORDER BY user_id, id
