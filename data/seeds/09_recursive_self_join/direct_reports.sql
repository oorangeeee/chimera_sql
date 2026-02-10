SELECT m.id AS manager_id, m.username AS manager, e.id AS employee_id, e.username AS employee FROM t_users e INNER JOIN t_users m ON e.manager_id = m.id ORDER BY m.id, e.id
