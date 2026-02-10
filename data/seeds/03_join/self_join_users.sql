SELECT a.id AS id1, b.id AS id2, a.username AS user1, b.username AS user2 FROM t_users a INNER JOIN t_users b ON a.age = b.age AND a.id < b.id ORDER BY a.id, b.id
