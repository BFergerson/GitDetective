CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE function_reference
(
    project_id         VARCHAR(255) NOT NULL,
    caller_function_id VARCHAR(255) NOT NULL,
    callee_function_id VARCHAR(255) NOT NULL,
    commit_sha1        VARCHAR(255) NOT NULL,
    commit_date        TIMESTAMPTZ  NOT NULL,
    line_number        INTEGER,
    deletion           BOOLEAN      NOT NULL
);
CREATE INDEX ON function_reference (project_id);
CREATE INDEX ON function_reference (project_id, callee_function_id);
CREATE INDEX ON function_reference (project_id, commit_sha1);
SELECT create_hypertable('function_reference', 'commit_date');
