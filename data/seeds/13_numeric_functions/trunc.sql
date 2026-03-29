SELECT id, username, TRUNC(score) AS score_int FROM t_users WHERE score IS NOT NULL ORDER BY id
