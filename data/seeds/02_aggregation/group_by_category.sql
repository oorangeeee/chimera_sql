SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM t_products WHERE category IS NOT NULL GROUP BY category ORDER BY category
