SELECT id, username, COALESCE(email, 'N/A') AS email_display FROM t_users ORDER BY id
