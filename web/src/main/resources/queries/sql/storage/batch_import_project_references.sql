--import references
CREATE TEMPORARY TABLE tmp_import_project_references (
  file_location           VARCHAR(2056) NOT NULL,
  x_file_or_function_name VARCHAR(510)  NOT NULL,
  x_qualified_name        VARCHAR(2056) NOT NULL,
  y_function_name         VARCHAR(510)  NOT NULL,
  y_qualified_name        VARCHAR(2056) NOT NULL,
  start_offset            INT           NOT NULL,
  end_offset              INT           NOT NULL,
  is_external             VARCHAR(5)    NOT NULL,
  is_jdk                  VARCHAR(5)    NOT NULL,
  PRIMARY KEY (x_file_or_function_name, y_function_name)
);

COPY tmp_import_project_references (file_location, x_file_or_function_name, x_qualified_name, y_function_name,
                                    y_qualified_name, start_offset, end_offset, is_external, is_jdk)
FROM '<inputFile>' DELIMITER '|' CSV HEADER;

--suppress references already imported
WITH tmp_import_project_references_ids AS (
    SELECT DISTINCT
      tmp_import_project_references.x_file_or_function_name,
      imported_file_or_function.x_file_or_function_id,
      tmp_import_project_references.y_function_name,
      imported_function.function_id y_function_id
    FROM tmp_import_project_references
      INNER JOIN (
                   SELECT
                     CASE WHEN x_file_or_function_name ~ '#'
                       THEN function_name
                     ELSE file_name END x_file_or_function_name,
                     CASE WHEN x_file_or_function_name ~ '#'
                       THEN function_id
                     ELSE file_id END   x_file_or_function_id
                   FROM tmp_import_project_references
                     LEFT JOIN imported_file
                       ON x_file_or_function_name = imported_file.file_name AND NOT x_file_or_function_name ~ '#'
                     LEFT JOIN imported_function AS imported_function_1
                       ON x_file_or_function_name = imported_function_1.function_name AND x_file_or_function_name ~ '#'
                 ) AS imported_file_or_function
        ON tmp_import_project_references.x_file_or_function_name = imported_file_or_function.x_file_or_function_name
      INNER JOIN imported_function ON y_function_name = imported_function.function_name
)
DELETE FROM tmp_import_project_references
USING tmp_import_project_references_ids, imported_reference
WHERE tmp_import_project_references.x_file_or_function_name = tmp_import_project_references_ids.x_file_or_function_name
      AND tmp_import_project_references.y_function_name = tmp_import_project_references_ids.y_function_name
      AND tmp_import_project_references_ids.x_file_or_function_id = imported_reference.file_or_function_id
      AND tmp_import_project_references_ids.y_function_id = imported_reference.function_id;

--export final file
COPY tmp_import_project_references (file_location, x_file_or_function_name, x_qualified_name, y_function_name,
                                    y_qualified_name, start_offset, end_offset, is_external, is_jdk)
TO '<outputFile>' DELIMITER '|' CSV HEADER;

DROP TABLE IF EXISTS tmp_import_project_references;