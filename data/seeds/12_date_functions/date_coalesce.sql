SELECT id, name, COALESCE(release_date, DATE '2000-01-01') AS effective_date FROM t_products ORDER BY id
