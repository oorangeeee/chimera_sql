SELECT id, initials, TRIM(initials) AS trimmed, LENGTH(TRIM(initials)) AS trimmed_len FROM t_users WHERE initials IS NOT NULL ORDER BY id
