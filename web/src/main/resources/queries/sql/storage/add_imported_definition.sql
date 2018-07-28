INSERT INTO imported_definition (file_id, function_id)
  VALUES (?, ?) ON CONFLICT DO NOTHING;