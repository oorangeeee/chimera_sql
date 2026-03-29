SELECT e.username AS employee, m.username AS manager FROM t_users e LEFT JOIN t_users m ON e.manager_id = m.id ORDER BY e.id
