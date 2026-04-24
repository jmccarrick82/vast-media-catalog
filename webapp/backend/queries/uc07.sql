-- UC07: Safe Deletion
-- Shows assets with their dependency analysis: dependent count,
-- whether they are leaf/root nodes, and whether deletion is safe.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.file_size_bytes,
    a.asset_classification,
    a.dependent_count,
    a.is_leaf,
    a.is_root,
    a.deletion_safe,
    a.deletion_evaluated_at
FROM vast."{{SCHEMA}}".assets a
WHERE a.deletion_evaluated_at IS NOT NULL
ORDER BY a.deletion_safe ASC, a.dependent_count DESC
LIMIT {{LIMIT}}
