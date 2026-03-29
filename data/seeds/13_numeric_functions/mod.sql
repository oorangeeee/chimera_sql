SELECT id, age, MOD(age, 10) AS age_mod FROM t_users WHERE age IS NOT NULL ORDER BY id
