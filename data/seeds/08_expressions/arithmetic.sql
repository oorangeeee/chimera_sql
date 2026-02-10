SELECT id, quantity, total_price, total_price / quantity AS unit_price FROM t_orders WHERE quantity > 0 ORDER BY id
