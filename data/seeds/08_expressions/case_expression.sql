SELECT id, username, age, CASE WHEN age < 18 THEN 'minor' WHEN age BETWEEN 18 AND 30 THEN 'young' WHEN age > 30 THEN 'senior' ELSE 'unknown' END AS age_group FROM t_users ORDER BY id
