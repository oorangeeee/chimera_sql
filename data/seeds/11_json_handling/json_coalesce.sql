SELECT id, username, COALESCE(json_extract(profile, '$.lang'), 'unknown') AS lang FROM t_users ORDER BY id
