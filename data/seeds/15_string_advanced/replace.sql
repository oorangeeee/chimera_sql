SELECT id, username, REPLACE(username, 'a', 'X') AS replaced FROM t_users ORDER BY id
