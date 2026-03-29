SELECT id, username, SUBSTR(username, -2, 2) AS last2 FROM t_users ORDER BY id
