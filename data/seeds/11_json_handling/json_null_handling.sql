SELECT id, username, profile, json_extract(profile, '$.theme') AS theme FROM t_users ORDER BY id
