DELETE FROM function_owner
WHERE 1=1
AND project_name = ?
AND function_id = ?
AND qualified_name = ?;