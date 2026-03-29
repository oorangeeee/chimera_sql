SELECT id, username, score, CAST(score AS INTEGER) + COALESCE(CAST(age AS INTEGER), 0) AS combo FROM t_users WHERE score IS NOT NULL ORDER BY id
