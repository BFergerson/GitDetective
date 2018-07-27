INSERT INTO imported_function (project_name, function_name, function_id)
  VALUES (?, ?, ?) ON CONFLICT DO NOTHING;