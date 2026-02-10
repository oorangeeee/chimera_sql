SELECT id, username, (score * 2 + COALESCE(age, 0)) AS composite FROM t_users WHERE score IS NOT NULL ORDER BY id
