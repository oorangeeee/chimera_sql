SELECT id, username, CAST(birth_date AS VARCHAR(30)) AS birth_str FROM t_users WHERE birth_date IS NOT NULL ORDER BY id
