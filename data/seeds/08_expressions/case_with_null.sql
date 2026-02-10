SELECT id, username, CASE WHEN score IS NULL THEN 'no_score' WHEN score >= 90 THEN 'excellent' WHEN score >= 60 THEN 'pass' ELSE 'fail' END AS grade FROM t_users ORDER BY id
