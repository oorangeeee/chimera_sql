SELECT o.id, u.username, p.name, o.quantity, o.total_price FROM t_orders o INNER JOIN t_users u ON o.user_id = u.id INNER JOIN t_products p ON o.product_id = p.id ORDER BY o.id
