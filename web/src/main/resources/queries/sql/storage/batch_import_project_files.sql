--import files
CREATE TEMPORARY TABLE tmp_import_project_files (
  file_location  VARCHAR(2056) NOT NULL,
  file_name      VARCHAR(510)  NOT NULL PRIMARY KEY,
  qualified_name VARCHAR(2056) NOT NULL
);

COPY tmp_import_project_files (file_location, file_name, qualified_name)
FROM '<inputFile>' DELIMITER '|' CSV HEADER;

--suppress files already imported
DELETE FROM tmp_import_project_files
USING imported_file
WHERE tmp_import_project_files.file_name = imported_file.file_name;

--export final file
COPY tmp_import_project_files (file_location, file_name, qualified_name)
TO '<outputFile>' DELIMITER '|' CSV HEADER;

DROP TABLE IF EXISTS tmp_import_project_files;