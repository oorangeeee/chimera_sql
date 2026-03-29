SELECT id, name, CAST(stock AS INTEGER) AS stock_int FROM t_products WHERE stock IS NOT NULL ORDER BY id
