SELECT reference_data::JSON
FROM function_reference
WHERE function_id = ?
ORDER BY CREATE_DATE ASC
OFFSET ?
LIMIT ?;