SELECT u.username, p.name, u.score + p.price AS combo FROM t_users u CROSS JOIN t_products p WHERE u.score IS NOT NULL AND p.price < 50 ORDER BY u.id, p.id
