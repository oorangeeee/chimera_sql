SELECT id, username FROM t_users WHERE json_extract(profile, '$.theme') = 'dark' ORDER BY id
