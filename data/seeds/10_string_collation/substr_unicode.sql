SELECT id, username, SUBSTR(username, 1, 3) AS prefix FROM t_users ORDER BY id
