SELECT id, username, score, PERCENT_RANK() OVER (ORDER BY score) AS pct_rank FROM t_users WHERE score IS NOT NULL ORDER BY pct_rank, id
