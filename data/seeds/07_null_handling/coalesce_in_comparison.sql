SELECT id, name, COALESCE(stock, 0) AS effective_stock FROM t_products WHERE COALESCE(stock, 0) > 0 ORDER BY id
