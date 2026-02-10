SELECT category, COUNT(*) AS cnt FROM t_products WHERE category IS NOT NULL GROUP BY category HAVING COUNT(*) > 1 ORDER BY category
