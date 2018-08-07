WITH function_ref_counts AS (
    SELECT
      function_id,
      COUNT(*) external_reference_count
    FROM function_reference
    GROUP BY function_id
)
SELECT
  function_id,
  external_reference_count
FROM function_ref_counts
ORDER by external_reference_count DESC
LIMIT ?;