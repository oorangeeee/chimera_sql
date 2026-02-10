SELECT id, username, json_extract(profile, '$.theme') AS theme, json_extract(profile, '$.lang') AS lang FROM t_users WHERE profile IS NOT NULL ORDER BY id
