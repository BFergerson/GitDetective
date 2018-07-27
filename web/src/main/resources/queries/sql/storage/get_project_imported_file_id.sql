SELECT file_id
FROM imported_file
WHERE 1=1
AND project_name = ?
AND file_name = ?;