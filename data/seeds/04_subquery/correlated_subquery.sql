SELECT id, username, score FROM t_users u WHERE score > (SELECT AVG(score) FROM t_users WHERE active = u.active AND score IS NOT NULL) ORDER BY id
