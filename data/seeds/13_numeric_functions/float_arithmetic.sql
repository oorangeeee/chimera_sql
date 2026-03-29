SELECT id, name, weight_kg, ROUND(weight_kg * 2.20462, 2) AS weight_lbs FROM t_products WHERE weight_kg IS NOT NULL ORDER BY id
