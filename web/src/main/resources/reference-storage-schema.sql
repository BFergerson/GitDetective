CREATE TABLE function_reference (
  create_date             TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  function_id             VARCHAR(255)  NOT NULL,
  reference_data          JSON          NOT NULL
);
CREATE INDEX ON function_reference (
  function_id
);

CREATE TABLE function_owner (
  create_date             TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  project_name            VARCHAR(510)  NOT NULL,
  function_id             VARCHAR(255)  NOT NULL,
  qualified_name          VARCHAR(2056)  NOT NULL,
  PRIMARY KEY (project_name, function_id)
);
CREATE INDEX ON function_owner (
  project_name
);
CREATE INDEX ON function_owner (
  function_id
);

CREATE TABLE imported_file (
  create_date             TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  project_name            VARCHAR(510)  NOT NULL,
  file_name               VARCHAR(255)  NOT NULL,
  file_id                 VARCHAR(255)  NOT NULL,
  PRIMARY KEY (project_name, file_name)
);

CREATE TABLE imported_function (
  create_date             TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  project_name            VARCHAR(510)  NOT NULL,
  function_name           VARCHAR(255)  NOT NULL,
  function_id             VARCHAR(255)  NOT NULL,
  PRIMARY KEY (project_name, function_name)
);

CREATE TABLE imported_definition (
  create_date             TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  file_id                 VARCHAR(255)  NOT NULL,
  function_id             VARCHAR(255)  NOT NULL,
  PRIMARY KEY (file_id, function_id)
);

CREATE TABLE imported_reference (
  create_date             TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  file_or_function_id     VARCHAR(255)  NOT NULL,
  function_id             VARCHAR(255)  NOT NULL,
  PRIMARY KEY (file_or_function_id, function_id)
);