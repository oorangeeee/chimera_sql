SELECT id, ROUND(POWER(price, 2), 4) AS price_sq FROM t_products WHERE price IS NOT NULL ORDER BY id
