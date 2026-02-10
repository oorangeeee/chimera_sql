SELECT id, username, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM t_users WHERE score IS NOT NULL ORDER BY drnk, id
