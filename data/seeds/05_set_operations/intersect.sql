SELECT tag FROM t_tags WHERE entity_type = 'user' INTERSECT SELECT tag FROM t_tags WHERE entity_type = 'product' ORDER BY tag
