SELECT id, username, score, (SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) AS avg_score FROM t_users WHERE score IS NOT NULL ORDER BY id
