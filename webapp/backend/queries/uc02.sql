-- UC02: Orphaned Asset Resolution
-- Shows assets that were previously orphaned and have been resolved,
-- including the resolution method and the asset they were linked back to.
SELECT
    a.asset_id,
    a.s3_path,
    a.filename,
    a.file_size_bytes,
    a.orphan_resolved_from_asset_id,
    a.orphan_resolution_method,
    a.orphan_resolved_at,
    p.filename AS resolved_from_filename,
    p.s3_path AS resolved_from_path,
    p.asset_classification AS resolved_from_classification
FROM vast."{{SCHEMA}}".assets a
LEFT JOIN vast."{{SCHEMA}}".assets p
    ON a.orphan_resolved_from_asset_id = p.asset_id
WHERE a.orphan_resolved_from_asset_id IS NOT NULL
ORDER BY a.orphan_resolved_at DESC
LIMIT {{LIMIT}}
