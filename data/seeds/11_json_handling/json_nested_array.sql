SELECT id, name, json_extract(metadata, '$.warranty') AS warranty FROM t_products WHERE metadata IS NOT NULL AND json_extract(metadata, '$.warranty') IS NOT NULL ORDER BY id
