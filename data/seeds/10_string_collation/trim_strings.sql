SELECT id, TRIM(username) AS trimmed_name, LENGTH(TRIM(username)) AS trimmed_len FROM t_users ORDER BY id
