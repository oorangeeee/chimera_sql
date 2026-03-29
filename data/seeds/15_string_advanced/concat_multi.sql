SELECT id, username || ' (' || COALESCE(email, 'N/A') || ')' AS contact FROM t_users ORDER BY id
