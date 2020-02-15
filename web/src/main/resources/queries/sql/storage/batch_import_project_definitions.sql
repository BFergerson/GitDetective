--import definitions
CREATE TEMPORARY TABLE tmp_import_project_definitions (
  file_name      VARCHAR(510)  NOT NULL,
  function_name  VARCHAR(510)  NOT NULL,
  qualified_name VARCHAR(2056) NOT NULL,
  start_offset   INT           NOT NULL,
  end_offset     INT           NOT NULL,
  PRIMARY KEY (file_name, function_name)
);

COPY tmp_import_project_definitions (file_name, function_name, qualified_name, start_offset, end_offset)
FROM '<inputFile>' DELIMITER '|' CSV HEADER;

--suppress definitions already imported
WITH tmp_import_project_definitions_ids AS (
    SELECT
      file_name,
      file_id,
      function_name,
      function_id
    FROM tmp_import_project_definitions
      INNER JOIN imported_file USING (file_name)
      INNER JOIN imported_function USING (function_name)
)
DELETE FROM tmp_import_project_definitions
USING tmp_import_project_definitions_ids, imported_definition
WHERE tmp_import_project_definitions.file_name = tmp_import_project_definitions_ids.file_name
AND tmp_import_project_definitions.function_name = tmp_import_project_definitions_ids.function_name
AND tmp_import_project_definitions_ids.file_id = imported_definition.file_id
AND tmp_import_project_definitions_ids.function_id = imported_definition.function_id;

--export final file
COPY tmp_import_project_definitions (file_name, function_name, qualified_name, start_offset, end_offset)
TO '<outputFile>' DELIMITER '|' CSV HEADER;

DROP TABLE IF EXISTS tmp_import_project_definitions;