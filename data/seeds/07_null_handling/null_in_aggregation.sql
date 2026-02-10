SELECT COUNT(*) AS total, COUNT(score) AS non_null_score, COUNT(*) - COUNT(score) AS null_score_count FROM t_users
