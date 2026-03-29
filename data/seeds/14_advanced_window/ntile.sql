SELECT id, username, score, NTILE(4) OVER (ORDER BY score) AS quartile FROM t_users WHERE score IS NOT NULL ORDER BY quartile, id
