SELECT id, height, CAST(height AS INTEGER) AS height_int FROM t_users WHERE height IS NOT NULL ORDER BY id
