SELECT id, quantity, total_price, SUM(total_price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS cumulative FROM t_orders ORDER BY id
