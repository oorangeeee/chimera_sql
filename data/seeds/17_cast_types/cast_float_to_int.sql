SELECT id, name, ROUND(weight_kg) AS weight_int FROM t_products WHERE weight_kg IS NOT NULL ORDER BY id
