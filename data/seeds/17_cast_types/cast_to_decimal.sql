SELECT id, username, CAST(age AS DECIMAL(5,2)) AS age_dec FROM t_users WHERE age IS NOT NULL ORDER BY id
