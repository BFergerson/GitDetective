INSERT INTO function_owner (project_name, function_id, qualified_name)
  VALUES (?, ?, ?) ON CONFLICT DO NOTHING ;