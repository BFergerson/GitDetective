DELETE FROM function_reference;

INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V2', 'V3', 'sha1', NOW(), 10, false);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V4', 'V3', 'sha2', NOW(), 10, false);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V5', 'V3', 'sha3', NOW(), 10, false);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V6', 'V3', 'sha4', NOW(), 10, false);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V7', 'V3', 'sha5', NOW(), 10, false);

INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V7', 'V3', 'sha6', NOW() + interval '1 month', 10, true);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V6', 'V3', 'sha7', NOW() + interval '1 month', 10, true);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V5', 'V3', 'sha8', NOW() + interval '1 month', 10, true);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V4', 'V3', 'sha9', NOW() + interval '1 month', 10, true);
INSERT INTO function_reference(project_id, caller_function_id, callee_function_id, commit_sha1, commit_date, line_number, deletion)
VALUES ('V1', 'V2', 'V3', 'sha10', NOW() + interval '1 month', 10, true);