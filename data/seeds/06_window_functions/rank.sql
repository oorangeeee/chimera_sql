SELECT id, username, score, RANK() OVER (ORDER BY score DESC) AS rnk FROM t_users WHERE score IS NOT NULL ORDER BY rnk, id
