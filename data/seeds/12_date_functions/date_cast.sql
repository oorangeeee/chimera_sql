SELECT id, username, birth_date, CAST(birth_date AS VARCHAR(20)) AS birth_str FROM t_users WHERE birth_date IS NOT NULL ORDER BY id
