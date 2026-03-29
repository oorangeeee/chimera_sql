SELECT entity_type, GROUP_CONCAT(tag, ', ') AS tags FROM t_tags WHERE entity_type = 'user' GROUP BY entity_type ORDER BY entity_type
