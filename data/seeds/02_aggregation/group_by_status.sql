SELECT status, COUNT(*) AS cnt, SUM(total_price) AS total FROM t_orders WHERE status IS NOT NULL GROUP BY status ORDER BY status
